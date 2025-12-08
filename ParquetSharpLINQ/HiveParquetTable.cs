using System.Collections;
using System.Linq.Expressions;
using System.Reflection;
using ParquetSharpLINQ.Attributes;
using ParquetSharpLINQ.Constants;
using ParquetSharpLINQ.Models;
using ParquetSharpLINQ.ParquetSharp;

namespace ParquetSharpLINQ;

public class HiveParquetTable<T> : IOrderedQueryable<T>, IDisposable where T : new()
{
    private readonly IParquetMapper<T> _mapper;
    private readonly IParquetReader _reader;
    private readonly string _rootPath;

    /// <summary>
    /// Creates a new HiveParquetTable for querying Parquet files with Hive-style partitioning.
    /// </summary>
    /// <param name="rootPath">Root directory containing Parquet files</param>
    /// <param name="reader">Optional custom Parquet reader (for testing/custom implementations)</param>
    /// <param name="mapper">Optional custom mapper (for DI/testing). If null, uses source-generated mapper.</param>
    public HiveParquetTable(string rootPath, IParquetReader? reader = null, IParquetMapper<T>? mapper = null)
    {
        _rootPath = rootPath ?? throw new ArgumentNullException(nameof(rootPath));
        _reader = reader ?? new ParquetSharpReader();
        Provider = new HiveParquetQueryProvider<T>(this);
        Expression = Expression.Constant(this);

        if (mapper != null)
        {
            _mapper = mapper;
        }
        else
        {
            var generatedMapperTypeName = $"{typeof(T).Namespace}.{typeof(T).Name}ParquetMapper";
            var generatedMapperType = typeof(T).Assembly.GetType(generatedMapperTypeName);

            if (generatedMapperType == null)
                throw new InvalidOperationException(
                    $"No source-generated mapper found for type {typeof(T).FullName}. " +
                    $"Make sure the type has properties with [ParquetColumn] attributes and the project is built.");

            _mapper = (IParquetMapper<T>)Activator.CreateInstance(generatedMapperType)!;
        }
    }

    public void Dispose()
    {
        var reader = _reader as IDisposable;
        reader?.Dispose();
    }

    public Type ElementType => typeof(T);

    public Expression Expression { get; }

    public IQueryProvider Provider { get; }

    public IEnumerator<T> GetEnumerator()
    {
        return AsEnumerable().GetEnumerator();
    }

    IEnumerator IEnumerable.GetEnumerator()
    {
        return GetEnumerator();
    }

    public IQueryable<T> AsQueryable()
    {
        return this;
    }

    public virtual IEnumerable<Partition> DiscoverPartitions()
    {
        return PartitionDiscovery.Discover(_rootPath);
    }

    public IEnumerable<T> AsEnumerable()
    {
        return AsEnumerable(null, null);
    }

    /// <summary>
    /// Optimized enumeration with partition pruning and column projection.
    /// </summary>
    internal IEnumerable<T> AsEnumerable(
        IReadOnlyDictionary<string, object?>? partitionFilters,
        IReadOnlyCollection<string>? requestedColumns)
    {
        return Enumerate(_mapper, partitionFilters, requestedColumns);

        IEnumerable<T> Enumerate(
            IParquetMapper<T> activeMapper,
            IReadOnlyDictionary<string, object?>? filters,
            IReadOnlyCollection<string>? columns)
        {
            var partitions = DiscoverPartitions();

            if (filters != null && filters.Count > 0)
            {
                var mappedFilters = MapPropertyNamesToColumnNames(filters);
                partitions = PrunePartitions(partitions, mappedFilters);
            }

            foreach (var partition in partitions)
            foreach (var file in _reader.ListFiles(partition.Path))
            {
                var availableColumnNames = _reader.GetColumns(file)
                    .Select(column => column.Name)
                    .Where(name => !string.IsNullOrWhiteSpace(name))
                    .ToList();

                var columnsToRead = ResolveColumnsToRead(activeMapper, availableColumnNames, columns);

                foreach (var row in _reader.ReadRows(file, columnsToRead))
                {
                    var enrichedRow = EnrichWithPartitionValues(row, partition.Values);
                    yield return activeMapper.Map(enrichedRow);
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

    private static Dictionary<string, object?> EnrichWithPartitionValues(
        IReadOnlyDictionary<string, object?> row,
        IReadOnlyDictionary<string, string> partitionValues)
    {
        var enriched = new Dictionary<string, object?>(row, StringComparer.OrdinalIgnoreCase);

        foreach (var (key, value) in partitionValues)
        {
            var partitionKey = $"{PartitionConstants.PartitionPrefix}{key}";
            enriched[partitionKey] = NormalizePartitionValue(value);
        }

        return enriched;
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
        var baseColumns = mapper.RequiredColumns is { Count: > 0 } requiredColumns
            ? requiredColumns
            : availableColumnNames;

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
                if (propertyToColumnMap.TryGetValue(propertyName, out var columnName))
                {
                    requestedColumnNames.Add(columnName);
                }
                else
                {
                    requestedColumnNames.Add(propertyName);
                }
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
}