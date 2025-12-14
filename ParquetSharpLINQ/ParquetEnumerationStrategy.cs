using ParquetSharpLINQ.Attributes;
using ParquetSharpLINQ.Constants;
using ParquetSharpLINQ.Models;
using ParquetSharpLINQ.ParquetSharp;
using System.Reflection;

namespace ParquetSharpLINQ;

/// <summary>
/// Encapsulates the strategy for enumerating Parquet data with partition pruning and column projection.
/// Used by ParquetTable with different discovery strategies (filesystem, Azure Blob, etc.).
/// </summary>
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

    /// <summary>
    /// Enumerates Parquet data with optional partition filtering and column projection.
    /// </summary>
    public IEnumerable<T> Enumerate(
        IReadOnlyDictionary<string, object?>? partitionFilters,
        IReadOnlyCollection<string>? requestedColumns)
    {
        var partitions = _discoveryStrategy.DiscoverPartitions();

        if (partitionFilters != null && partitionFilters.Count > 0)
        {
            var mappedFilters = MapPropertyNamesToColumnNames(partitionFilters);
            partitions = PrunePartitions(partitions, mappedFilters);
        }

        // Optimization: If all requested columns are partition columns, return data from partition metadata only
        if (requestedColumns != null && requestedColumns.Count > 0 && AreAllColumnsPartitions(requestedColumns))
        {
            foreach (var partition in partitions)
            {
                var columnNames = new string[partition.Values.Count];
                var values = new object?[partition.Values.Count];
                var index = 0;

                foreach (var (key, value) in partition.Values)
                {
                    columnNames[index] = $"{PartitionConstants.PartitionPrefix}{key}";
                    values[index] = NormalizePartitionValue(value);
                    index++;
                }

                var row = new ParquetRow(columnNames, values);
                yield return _mapper.Map(row);
            }
            yield break; // Early exit - no file reading needed!
        }

        foreach (var partition in partitions)
        {
            var filesToRead = partition.Files;

            foreach (var file in filesToRead)
            {
                var availableColumnNames = _reader.GetColumns(file)
                    .Select(column => column.Name)
                    .Where(name => !string.IsNullOrWhiteSpace(name))
                    .ToList();

                var columnsToRead = ResolveColumnsToRead(_mapper, availableColumnNames, requestedColumns);

                foreach (var row in _reader.ReadRows(file, columnsToRead))
                {
                    var enrichedRow = EnrichWithPartitionValues(row, partition.Values);
                    yield return _mapper.Map(enrichedRow);
                }
            }
        }
    }

    private static IEnumerable<Partition> PrunePartitions(
        IEnumerable<Partition> partitions,
        IReadOnlyDictionary<string, object?> filters)
    {
        foreach (var partition in partitions)
        {
            var matches = true;

            foreach (var (key, expectedValue) in filters)
            {
                if (partition.Values.TryGetValue(key, out var actualValue))
                {
                    if (!ValuesMatch(actualValue, expectedValue))
                    {
                        matches = false;
                        break;
                    }
                }
            }

            if (matches)
            {
                yield return partition;
            }
        }
    }

    private static Dictionary<string, object?> MapPropertyNamesToColumnNames(
        IReadOnlyDictionary<string, object?> propertyFilters)
    {
        var mapped = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);

        foreach (var (propertyName, value) in propertyFilters)
        {
            var property = typeof(T).GetProperty(propertyName, BindingFlags.Public | BindingFlags.Instance);
            if (property == null)
            {
                mapped[propertyName] = NormalizeFilterValue(value);
                continue;
            }

            var attr = property.GetCustomAttribute<ParquetColumnAttribute>();
            var columnName = attr?.Name ?? propertyName;

            mapped[columnName] = NormalizeFilterValue(value);
        }

        return mapped;
    }

    private static object? NormalizeFilterValue(object? value)
    {
        if (value is not string strValue)
        {
            return value;
        }

        if (DateTime.TryParse(strValue, out _) ||
            DateOnly.TryParse(strValue, out _) ||
            long.TryParse(strValue, out _))
        {
            return strValue;
        }

        return strValue.ToLowerInvariant();
    }

    private static bool ValuesMatch(string partitionValue, object? filterValue)
    {
        if (filterValue == null)
        {
            return string.IsNullOrEmpty(partitionValue);
        }

        if (IsNumeric(filterValue))
        {
            if (long.TryParse(partitionValue, out var partitionNumeric))
            {
                var filterNumeric = Convert.ToInt64(filterValue);
                return partitionNumeric == filterNumeric;
            }
        }

        if (filterValue is DateTime filterDateTime)
        {
            if (DateTime.TryParse(partitionValue, out var partitionDateTime))
            {
                return partitionDateTime == filterDateTime;
            }
        }

        if (filterValue is DateOnly filterDateOnly)
        {
            if (DateOnly.TryParse(partitionValue, out var partitionDateOnly))
            {
                return partitionDateOnly == filterDateOnly;
            }
        }

        var filterString = filterValue.ToString();
        return string.Equals(partitionValue, filterString, StringComparison.OrdinalIgnoreCase);
    }

    private static bool IsNumeric(object value)
    {
        return value is sbyte or byte or short or ushort or int or uint or long or ulong or float or double or decimal;
    }

    private static ParquetRow EnrichWithPartitionValues(
        ParquetRow row,
        IReadOnlyDictionary<string, string> partitionValues)
    {
        if (partitionValues.Count == 0)
        {
            return row;
        }

        var totalColumns = row.ColumnCount + partitionValues.Count;
        var enrichedColumnNames = new string[totalColumns];
        var enrichedValues = new object?[totalColumns];

        // Copy existing row data
        var columnNames = row.ColumnNames;
        var values = row.Values;
        for (var i = 0; i < row.ColumnCount; i++)
        {
            enrichedColumnNames[i] = columnNames[i];
            enrichedValues[i] = values[i];
        }

        // Add partition values
        var offset = row.ColumnCount;
        var index = 0;
        foreach (var (key, value) in partitionValues)
        {
            var partitionKey = $"{PartitionConstants.PartitionPrefix}{key}";
            enrichedColumnNames[offset + index] = partitionKey;
            enrichedValues[offset + index] = NormalizePartitionValue(value);
            index++;
        }

        return new ParquetRow(enrichedColumnNames, enrichedValues);
    }

    private static string NormalizePartitionValue(string value)
    {
        if (DateTime.TryParse(value, out _) ||
            DateOnly.TryParse(value, out _) ||
            long.TryParse(value, out _))
        {
            return value;
        }

        return value.ToLowerInvariant();
    }

    private static IReadOnlyList<string> ResolveColumnsToRead(
        IParquetMapper<T> mapper,
        IReadOnlyList<string> availableColumnNames,
        IReadOnlyCollection<string>? requestedColumns)
    {
        var baseColumns = mapper.RequiredColumns ?? availableColumnNames;

        var normalizedBase = baseColumns
            .Where(name => !string.IsNullOrWhiteSpace(name))
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToList();

        if (requestedColumns != null && requestedColumns.Count > 0)
        {
            var propertyToColumnMap = BuildPropertyToColumnMap();
            var requestedColumnNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

            foreach (var propertyName in requestedColumns)
            {
                var columnName = propertyToColumnMap.GetValueOrDefault(propertyName, propertyName);
                requestedColumnNames.Add(columnName);
            }

            normalizedBase = normalizedBase
                .Where(col => requestedColumnNames.Contains(col))
                .ToList();
        }

        if (normalizedBase.Count == 0)
        {
            return availableColumnNames;
        }

        var availableSet = new HashSet<string>(availableColumnNames, StringComparer.OrdinalIgnoreCase);
        var missing = normalizedBase
            .Where(column => !availableSet.Contains(column))
            .ToList();

        if (missing.Count > 0)
        {
            throw new InvalidOperationException(
                $"Parquet file is missing required columns: {string.Join(", ", missing)}");
        }

        return normalizedBase;
    }

    private static Dictionary<string, string> BuildPropertyToColumnMap()
    {
        var map = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        var properties = typeof(T).GetProperties(BindingFlags.Instance | BindingFlags.Public);

        foreach (var property in properties)
        {
            var attr = property.GetCustomAttribute<ParquetColumnAttribute>();
            if (attr != null)
            {
                var columnName = string.IsNullOrWhiteSpace(attr.Name) ? property.Name : attr.Name;
                map[property.Name] = columnName;
            }
        }

        return map;
    }

    private static bool AreAllColumnsPartitions(IReadOnlyCollection<string> requestedColumns)
    {
        var properties = typeof(T).GetProperties(BindingFlags.Instance | BindingFlags.Public);
        var partitionColumns = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        foreach (var property in properties)
        {
            var attr = property.GetCustomAttribute<ParquetColumnAttribute>();
            if (attr?.IsPartition == true)
            {
                partitionColumns.Add(property.Name);
            }
        }

        return requestedColumns.All(col => partitionColumns.Contains(col));
    }
}
