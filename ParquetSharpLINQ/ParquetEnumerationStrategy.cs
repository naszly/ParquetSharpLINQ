using System.Collections.Immutable;
using System.Runtime.CompilerServices;
using System.Threading.Channels;
using ParquetSharpLINQ.Discovery;
using ParquetSharpLINQ.Enumeration;
using ParquetSharpLINQ.Enumeration.Indexing;
using ParquetSharpLINQ.Interfaces;
using ParquetSharpLINQ.Models;
using ParquetSharpLINQ.ParquetSharp;
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
    private readonly IndexedPredicateEngine<T> _indexedPredicateEngine;

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
        _indexedPredicateEngine = new IndexedPredicateEngine<T>(_parquetReader);
        
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

        var predicatePropertyNames = GetPredicatePropertyNames(predicates);
        var rowPredicates = RowPredicateBuilder<T>.BuildRowPredicates(predicates);
        var indexedConstraints = _indexedPredicateEngine.BuildIndexedPredicateConstraints(predicates, allowWarmup: true);

        if (predicates is { Count: > 0 })
        {
            partitions = FilterPartitions(partitions, predicates);
        }

        if (requestedColumns is { Count: > 0 }
            && PropertyColumnMapper<T>.AreAllColumnsPartitions(requestedColumns)
            && (predicatePropertyNames.Count == 0 || PropertyColumnMapper<T>.AreAllColumnsPartitions(predicatePropertyNames)))
        {
            return EnumerateFromPartitionMetadataOnly(partitions);
        }

        var effectiveRequestedColumns = MergeRequestedColumns(requestedColumns, predicatePropertyNames);

        if (_degreeOfParallelism > 1)
        {
            return EnumerateFromParquetParallelAsync(
                    partitions,
                    effectiveRequestedColumns,
                    indexedConstraints,
                    rowPredicates,
                    rangeFilters,
                    _degreeOfParallelism)
                .ToBlockingEnumerable();
        }

        return EnumerateFromParquet(partitions, effectiveRequestedColumns, indexedConstraints, rowPredicates, rangeFilters);
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

            _discoveredPartitions = [.._partitionDiscoveryStrategy.DiscoverPartitions()];
            _lastPartitionDiscoveryTime = now;
            ClearIndexedColumnCache();

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
        IReadOnlyList<IndexedPredicateConstraint> indexedConstraints,
        IReadOnlyList<Func<ParquetRow, bool>> rowPredicates,
        IReadOnlyDictionary<string, RangeFilter>? rangeFilters)
    {
        foreach (var partition in partitions)
        {
            var filesToRead = ApplyStatisticsBasedPruning(partition.Files, rangeFilters);

            foreach (var file in filesToRead)
            {
                foreach (var entity in ReadEntities(file, partition, requestedColumns, indexedConstraints, rowPredicates))
                {
                    yield return entity;
                }
            }
        }
    }

    private async IAsyncEnumerable<T> EnumerateFromParquetParallelAsync(
        IEnumerable<Partition> partitions,
        IReadOnlyCollection<string>? requestedColumns,
        IReadOnlyList<IndexedPredicateConstraint> indexedConstraints,
        IReadOnlyList<Func<ParquetRow, bool>> rowPredicates,
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

                    foreach (var entity in ReadEntities(file, partition, requestedColumns, indexedConstraints, rowPredicates))
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
        IReadOnlyCollection<string>? requestedColumns,
        IReadOnlyList<IndexedPredicateConstraint> indexedConstraints,
        IReadOnlyList<Func<ParquetRow, bool>> rowPredicates)
    {
        var availableColumnNames = GetAvailableColumnNames(file.Path);
        var columnsToRead = ColumnResolver<T>.ResolveColumnsToRead(_parquetMapper, availableColumnNames, requestedColumns)
            .ToArray();

        var rowGroupsToRead = ResolveRowGroupsToRead(file.Path, indexedConstraints);
        if (rowGroupsToRead is { Count: 0 })
            yield break;

        var rows = rowGroupsToRead == null
            ? _parquetReader.ReadRows(file.Path, columnsToRead)
            : _parquetReader.ReadRows(file.Path, columnsToRead, rowGroupsToRead);

        foreach (var row in rows)
        {
            var enrichedRow = PartitionRowEnricher.EnrichWithPartitionValues(row, partition.Values);
            if (!RowMatchesPredicates(rowPredicates, enrichedRow))
                continue;

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

    private static HashSet<string> GetPredicatePropertyNames(IReadOnlyCollection<QueryPredicate>? predicates)
    {
        var propertyNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        if (predicates == null || predicates.Count == 0)
            return propertyNames;

        foreach (var predicate in predicates)
        {
            foreach (var property in predicate.Properties)
            {
                propertyNames.Add(property.Name);
            }
        }

        return propertyNames;
    }


    private static bool RowMatchesPredicates(
        IReadOnlyList<Func<ParquetRow, bool>> predicates,
        ParquetRow row)
    {
        return predicates.All(predicate => predicate(row));
    }

    public bool TryCountUsingIndex(
        IReadOnlyCollection<QueryPredicate>? predicates,
        IReadOnlyDictionary<string, RangeFilter>? rangeFilters,
        out long count)
    {
        count = 0;
        if (predicates == null || predicates.Count == 0)
            return false;

        var constraints = _indexedPredicateEngine.BuildIndexedPredicateConstraints(predicates, allowWarmup: false);
        if (constraints.Count != 1)
            return false;

        var constraint = constraints[0];
        var partitions = DiscoverPartitions();
        if (predicates.Count > 0)
            partitions = FilterPartitions(partitions, predicates);

        foreach (var partition in partitions)
        {
            var filesToRead = ApplyStatisticsBasedPruning(partition.Files, rangeFilters);
            foreach (var file in filesToRead)
            {
                foreach (var values in _indexedPredicateEngine.GetRowGroupValues(file.Path, constraint.Definition))
                {
                    if (!constraint.TryCountMatches(values, out var groupCount))
                        return false;

                    count += groupCount;
                }
            }
        }

        return true;
    }

    private static IReadOnlyCollection<string>? MergeRequestedColumns(
        IReadOnlyCollection<string>? requestedColumns,
        IReadOnlyCollection<string> predicatePropertyNames)
    {
        if (requestedColumns == null)
            return null;

        if (predicatePropertyNames.Count == 0)
            return requestedColumns;

        var merged = new HashSet<string>(requestedColumns, StringComparer.OrdinalIgnoreCase);
        foreach (var predicateProperty in predicatePropertyNames)
        {
            merged.Add(predicateProperty);
        }

        return merged;
    }

    private void ClearIndexedColumnCache()
    {
        _indexedPredicateEngine.ClearCache();
    }

    private IReadOnlySet<int>? ResolveRowGroupsToRead(
        string filePath,
        IReadOnlyList<IndexedPredicateConstraint> constraints)
    {
        return _indexedPredicateEngine.ResolveRowGroupsToRead(filePath, constraints);
    }
}
