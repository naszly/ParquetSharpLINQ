using System.Collections.Immutable;
using ParquetSharpLINQ.Discovery;
using ParquetSharpLINQ.Enumeration;
using ParquetSharpLINQ.Interfaces;
using ParquetSharpLINQ.Models;
using ParquetSharpLINQ.Query;

namespace ParquetSharpLINQ;

internal class ParquetEnumerationStrategy<T> where T : new()
{
    private readonly IPartitionDiscoveryStrategy _discoveryStrategy;
    private readonly IParquetReader _reader;
    private readonly IParquetMapper<T> _mapper;

    public ParquetEnumerationStrategy(
        IPartitionDiscoveryStrategy discoveryStrategy,
        IParquetReader reader,
        IParquetMapper<T> mapper)
    {
        _discoveryStrategy = discoveryStrategy ?? throw new ArgumentNullException(nameof(discoveryStrategy));
        _reader = reader ?? throw new ArgumentNullException(nameof(reader));
        _mapper = mapper ?? throw new ArgumentNullException(nameof(mapper));
    }

    public IEnumerable<T> Enumerate(
        IReadOnlyDictionary<string, object?>? partitionFilters,
        IReadOnlyCollection<string>? requestedColumns,
        IReadOnlyDictionary<string, RangeFilter>? rangeFilters = null)
    {
        var partitions = DiscoverAndFilterPartitions(partitionFilters);

        if (requestedColumns != null && requestedColumns.Count > 0 && 
            PropertyColumnMapper<T>.AreAllColumnsPartitions(requestedColumns))
        {
            return EnumerateFromPartitionMetadataOnly(partitions);
        }

        return EnumerateFromParquetFiles(partitions, requestedColumns, rangeFilters);
    }

    private IEnumerable<Partition> DiscoverAndFilterPartitions(IReadOnlyDictionary<string, object?>? partitionFilters)
    {
        var partitions = _discoveryStrategy.DiscoverPartitions();

        if (partitionFilters != null && partitionFilters.Count > 0)
        {
            var mappedFilters = PropertyColumnMapper<T>.MapPropertyNamesToColumnNames(partitionFilters);
            partitions = PartitionFilter.PrunePartitions(partitions, mappedFilters);
        }

        return partitions;
    }

    private IEnumerable<T> EnumerateFromPartitionMetadataOnly(IEnumerable<Partition> partitions)
    {
        return partitions
            .Select(PartitionRowEnricher.CreateRowFromPartitionMetadata)
            .Select(row => _mapper.Map(row));
    }


    private IEnumerable<T> EnumerateFromParquetFiles(
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
        var columnsToRead = ColumnResolver<T>.ResolveColumnsToRead(_mapper, availableColumnNames, requestedColumns);

        foreach (var row in _reader.ReadRows(file.Path, columnsToRead))
        {
            var enrichedRow = PartitionRowEnricher.EnrichWithPartitionValues(row, partition.Values);
            yield return _mapper.Map(enrichedRow);
        }
    }

    private IReadOnlyList<string> GetAvailableColumnNames(string filePath)
    {
        return _reader.GetColumns(filePath)
            .Select(column => column.Name)
            .Where(name => !string.IsNullOrWhiteSpace(name))
            .ToImmutableArray();
    }
}
