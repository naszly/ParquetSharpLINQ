using System.Collections.Immutable;
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

    public ParquetEnumerationStrategy(
        IPartitionDiscoveryStrategy discoveryStrategy,
        IParquetReader reader,
        IParquetMapper<T> mapper,
        TimeSpan? partitionCacheDuration = null)
    {
        _partitionDiscoveryStrategy = discoveryStrategy ?? throw new ArgumentNullException(nameof(discoveryStrategy));
        _parquetReader = reader ?? throw new ArgumentNullException(nameof(reader));
        _parquetMapper = mapper ?? throw new ArgumentNullException(nameof(mapper));
        
        if (partitionCacheDuration.HasValue)
        {
            _partitionCacheDuration = partitionCacheDuration.Value;
        }
    }

    public IEnumerable<T> Enumerate(
        IReadOnlyDictionary<string, object?>? partitionFilters = null,
        IReadOnlyCollection<string>? requestedColumns = null,
        IReadOnlyDictionary<string, RangeFilter>? rangeFilters = null)
    {
        var partitions = DiscoverPartitions();
        
        if (partitionFilters is { Count: > 0 })
        {
            partitions = FilterPartitions(partitions, partitionFilters);
        }

        if (requestedColumns is { Count: > 0 } && PropertyColumnMapper<T>.AreAllColumnsPartitions(requestedColumns))
        {
            return EnumerateFromPartitionMetadataOnly(partitions);
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
        IReadOnlyDictionary<string, object?> partitionFilters)
    {
        var mappedFilters = PropertyColumnMapper<T>.MapPropertyNamesToColumnNames(partitionFilters);
        return PartitionFilter.PrunePartitions(partitions, mappedFilters);
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
                foreach (var entity in ReadEntitiesFromFile(file, partition, requestedColumns))
                {
                    yield return entity;
                }
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

    private IEnumerable<T> ReadEntitiesFromFile(
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
