using ParquetSharpLINQ.Attributes;
using ParquetSharpLINQ.Models;
using System.Collections;
using System.Linq.Expressions;
using System.Reflection;

namespace ParquetSharpLINQ;

public sealed class ParquetTable<T> : IOrderedQueryable<T>, IDisposable where T : new()
{
    private readonly IParquetReader _reader;
    private readonly IPartitionDiscoveryStrategy _discoveryStrategy;
    private readonly ParquetEnumerationStrategy<T> _enumerationStrategy;
    private readonly SemaphoreSlim _prefetchLock = new(1, 1);

    /// <summary>
    /// Gets the factory for creating ParquetTable instances.
    /// Use ParquetTable&lt;T&gt;.Factory.FromFileSystem() or ParquetTable&lt;T&gt;.Factory.FromAzureBlob() (with Azure package).
    /// </summary>
    public static ParquetTableFactory<T> Factory { get; } = new();
    
    /// <summary>
    /// Creates a new ParquetTable with custom strategy and reader implementations.
    /// Advanced constructor for dependency injection and testing scenarios.
    /// </summary>
    /// <param name="discoveryStrategy">Partition discovery strategy</param>
    /// <param name="reader">Parquet reader implementation</param>
    /// <param name="mapper">Optional custom mapper (for DI/testing). If null, uses source-generated mapper.</param>
    public ParquetTable(
        IPartitionDiscoveryStrategy discoveryStrategy,
        IParquetReader reader,
        IParquetMapper<T>? mapper = null)
    {
        _discoveryStrategy = discoveryStrategy ?? throw new ArgumentNullException(nameof(discoveryStrategy));
        _reader = reader ?? throw new ArgumentNullException(nameof(reader));
        _enumerationStrategy = new ParquetEnumerationStrategy<T>(_discoveryStrategy, _reader, mapper ?? ResolveMapper());
        Provider = new ParquetQueryProvider<T>(_enumerationStrategy);
        Expression = Expression.Constant(this);
    }

    private static IParquetMapper<T> ResolveMapper()
    {
        var generatedMapperTypeName = $"{typeof(T).Namespace}.{typeof(T).Name}ParquetMapper";
        var generatedMapperType = typeof(T).Assembly.GetType(generatedMapperTypeName);

        if (generatedMapperType == null)
            throw new InvalidOperationException(
                $"No source-generated mapper found for type {typeof(T).FullName}. " +
                $"Make sure the type has properties with [ParquetColumn] attributes and the project is built.");

        return (IParquetMapper<T>)Activator.CreateInstance(generatedMapperType)!;
    }

    public void Dispose()
    {
        var reader = _reader as IDisposable;
        reader?.Dispose();
        _prefetchLock.Dispose();
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

    public IEnumerable<T> AsEnumerable()
    {
        return AsEnumerable(null, null);
    }

    /// <summary>
    /// Discovers all partitions in the table.
    /// Useful for inspecting partition structure and debugging.
    /// </summary>
    public IEnumerable<Partition> DiscoverPartitions()
    {
        return _discoveryStrategy.DiscoverPartitions();
    }

    /// <summary>
    /// Clears any cached Delta log data, forcing fresh reads on next query.
    /// Only has an effect for Delta tables.
    /// </summary>
    public void ClearDeltaLogCache()
    {
        _discoveryStrategy.ClearDeltaLogCache();
    }

    /// <summary>
    /// Optimized enumeration with partition pruning and column projection.
    /// </summary>
    /// <param name="partitionFilters">Filters for partition pruning. Null = no filtering.</param>
    /// <param name="requestedColumns">Columns to read. Null = all entity columns, non-null = specific columns.</param>
    internal IEnumerable<T> AsEnumerable(
        IReadOnlyDictionary<string, object?>? partitionFilters,
        IReadOnlyCollection<string>? requestedColumns)
    {
        if (_reader is IAsyncParquetReader asyncReader)
        {
            var partitions = _discoveryStrategy.DiscoverPartitions();

            if (partitionFilters != null && partitionFilters.Count > 0)
            {
                var mappedFilters = MapPropertyNamesToColumnNames(partitionFilters);
                partitions = PrunePartitions(partitions, mappedFilters);
            }

            if (requestedColumns == null || requestedColumns.Count == 0 || !AreAllColumnsPartitions(requestedColumns))
            {
                var filesToPrefetch = partitions.SelectMany(p => p.Files).ToList();

                _ = Task.Run(async () =>
                {
                    try
                    {
                        await asyncReader.PrefetchAsync(filesToPrefetch);
                    }
                    catch
                    {
                        // Swallow prefetch errors - enumeration will still work from cache or download on-demand
                    }
                });
            }
        }

        return _enumerationStrategy.Enumerate(partitionFilters, requestedColumns);
    }

    // Helper methods for prefetch logic

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