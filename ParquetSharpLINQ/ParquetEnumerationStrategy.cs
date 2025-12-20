using System.Collections.Immutable;
using System.Runtime.CompilerServices;
using System.Threading.Channels;
using ParquetSharpLINQ.Discovery;
using ParquetSharpLINQ.Enumeration;
using ParquetSharpLINQ.Interfaces;
using ParquetSharpLINQ.Models;
using ParquetSharpLINQ.Query;

namespace ParquetSharpLINQ;

#if !NET9_0_OR_GREATER
using Lock = object;
#endif

internal class ParquetEnumerationStrategy<T> where T : new()
{
    private readonly IPartitionDiscoveryStrategy _partitionDiscoveryStrategy;
    private readonly IParquetReader _parquetReader;
    private readonly IParquetMapper<T> _parquetMapper;
    
    private ImmutableArray<Partition> _discoveredPartitions = [];
    private DateTime _lastPartitionDiscoveryTime = DateTime.MinValue;
    private readonly TimeSpan _partitionCacheDuration = TimeSpan.FromMinutes(5);
    private readonly Lock _partitionDiscoveryLock = new();

    private readonly int _degreeOfParallelism;

    public ParquetEnumerationStrategy(
        IPartitionDiscoveryStrategy discoveryStrategy,
        IParquetReader reader,
        IParquetMapper<T> mapper,
        TimeSpan? partitionCacheDuration = null,
        int degreeOfParallelism = 0)
    {
        _partitionDiscoveryStrategy = discoveryStrategy ?? throw new ArgumentNullException(nameof(discoveryStrategy));
        _parquetReader = reader ?? throw new ArgumentNullException(nameof(reader));
        _parquetMapper = mapper ?? throw new ArgumentNullException(nameof(mapper));
        
        if (partitionCacheDuration.HasValue)
        {
            _partitionCacheDuration = partitionCacheDuration.Value;
        }

        _degreeOfParallelism = degreeOfParallelism;
    }

    public IEnumerable<T> Enumerate(
        IReadOnlyCollection<QueryPredicate>? predicates = null,
        IReadOnlyCollection<string>? requestedColumns = null,
        IReadOnlyDictionary<string, RangeFilter>? rangeFilters = null)
    {
        var partitions = DiscoverPartitions();
        
        if (predicates is { Count: > 0 })
        {
            partitions = FilterPartitions(partitions, predicates);
        }

        if (requestedColumns is { Count: > 0 } && PropertyColumnMapper<T>.AreAllColumnsPartitions(requestedColumns))
        {
            return EnumerateFromPartitionMetadataOnly(partitions);
        }

        if (_degreeOfParallelism > 1)
        {
            return EnumerateFromParquetParallelAsync(partitions, requestedColumns, rangeFilters, _degreeOfParallelism)
                .ToBlockingEnumerable();
        }

        return EnumerateFromParquet(partitions, requestedColumns, rangeFilters);
    }
    
    /// <summary>
    /// Discovers and returns all partitions for the table using the configured discovery strategy.
    /// Results are cached and the method is thread-safe.
    /// </summary>
    public IEnumerable<Partition> DiscoverPartitions()
    {
        lock (_partitionDiscoveryLock)
        {
            var now = DateTime.UtcNow;
            if (now - _lastPartitionDiscoveryTime <= _partitionCacheDuration)
            {
                return _discoveredPartitions;
            }

            _discoveredPartitions = _partitionDiscoveryStrategy.DiscoverPartitions().ToImmutableArray();
            _lastPartitionDiscoveryTime = now;

            return _discoveredPartitions;
        }
    }

    private static IEnumerable<Partition> FilterPartitions(
        IEnumerable<Partition> partitions,
        IReadOnlyCollection<QueryPredicate> predicates)
    {
        return PartitionFilter.PrunePartitions<T>(partitions, predicates);
    }

    private IEnumerable<T> EnumerateFromPartitionMetadataOnly(IEnumerable<Partition> partitions)
    {
        return partitions
            .Select(PartitionRowEnricher.CreateRowFromPartitionMetadata)
            .Select(row => _parquetMapper.Map(row));
    }

    private IEnumerable<T> EnumerateFromParquet(
        IEnumerable<Partition> partitions,
        IReadOnlyCollection<string>? requestedColumns,
        IReadOnlyDictionary<string, RangeFilter>? rangeFilters)
    {
        foreach (var partition in partitions)
        {
            var filesToRead = ApplyStatisticsBasedPruning(partition.Files, rangeFilters);

            foreach (var file in filesToRead)
            {
                foreach (var entity in ReadEntities(file, partition, requestedColumns))
                {
                    yield return entity;
                }
            }
        }
    }

    private async IAsyncEnumerable<T> EnumerateFromParquetParallelAsync(
        IEnumerable<Partition> partitions,
        IReadOnlyCollection<string>? requestedColumns,
        IReadOnlyDictionary<string, RangeFilter>? rangeFilters,
        int degreeOfParallelism,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        if (degreeOfParallelism <= 0)
            throw new ArgumentOutOfRangeException(nameof(degreeOfParallelism));

        var jobs = partitions
            .SelectMany(p =>
            {
                var pruned = ApplyStatisticsBasedPruning(p.Files, rangeFilters);
                return pruned.Select(f => (Partition: p, File: f));
            })
            .OrderByDescending(x => x.File.SizeBytes ?? long.MaxValue);

        // Linked token that we can cancel when the enumerator is disposed early
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var token = cts.Token;

        var results = Channel.CreateBounded<T>(new BoundedChannelOptions(1024)
        {
            SingleReader = true,
            SingleWriter = false,
            FullMode = BoundedChannelFullMode.Wait
        });

        var producer = Task.Run(async () =>
        {
            try
            {
                var parallelOptions = new ParallelOptions
                {
                    MaxDegreeOfParallelism = degreeOfParallelism,
                    CancellationToken = token
                };

                async ValueTask ProcessJob((Partition, ParquetFile) job, CancellationToken ct)
                {
                    var (partition, file) = job;

                    foreach (var entity in ReadEntities(file, partition, requestedColumns))
                    {
                        ct.ThrowIfCancellationRequested();
                        await results.Writer.WriteAsync(entity, ct).ConfigureAwait(false);
                    }
                }

                await Parallel.ForEachAsync(jobs, parallelOptions, ProcessJob).ConfigureAwait(false);
                results.Writer.TryComplete();
            }
            catch (OperationCanceledException oce) when (oce.CancellationToken == token)
            {
                // Expected if the consumer stops early
                results.Writer.TryComplete(oce);
            }
            catch (Exception ex)
            {
                results.Writer.TryComplete(ex);
            }
        }, CancellationToken.None);

        try
        {
            await foreach (var item in results.Reader.ReadAllAsync(token).ConfigureAwait(false))
                yield return item;
        }
        finally
        {
            // This runs when the async enumerator is disposed (e.g. Take/First stops early)
            cts.Cancel();
            results.Writer.TryComplete();

            try
            {
                await producer.ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (token.IsCancellationRequested)
            {
                // ignore
            }
        }
    }

    private IEnumerable<ParquetFile> ApplyStatisticsBasedPruning(
        IEnumerable<ParquetFile> files,
        IReadOnlyDictionary<string, RangeFilter>? rangeFilters)
    {
        if (rangeFilters == null || rangeFilters.Count == 0)
        {
            return files;
        }

        var mappedRangeFilters = PropertyColumnMapper<T>.MapRangeFilterPropertyNamesToColumnNames(rangeFilters);
        return StatisticsBasedFilePruner.PruneFilesByStatistics(files, mappedRangeFilters);
    }

    private IEnumerable<T> ReadEntities(
        ParquetFile file,
        Partition partition,
        IReadOnlyCollection<string>? requestedColumns)
    {
        var availableColumnNames = GetAvailableColumnNames(file.Path);
        var columnsToRead = ColumnResolver<T>.ResolveColumnsToRead(_parquetMapper, availableColumnNames, requestedColumns);

        foreach (var row in _parquetReader.ReadRows(file.Path, columnsToRead))
        {
            var enrichedRow = PartitionRowEnricher.EnrichWithPartitionValues(row, partition.Values);
            yield return _parquetMapper.Map(enrichedRow);
        }
    }

    private IReadOnlyList<string> GetAvailableColumnNames(string filePath)
    {
        return _parquetReader.GetColumns(filePath)
            .Select(column => column.Name)
            .Where(name => !string.IsNullOrWhiteSpace(name))
            .ToImmutableArray();
    }
}
