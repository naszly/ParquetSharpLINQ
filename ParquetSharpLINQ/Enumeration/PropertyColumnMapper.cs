using System.Collections.Concurrent;
using System.Collections.Immutable;
using ParquetSharpLINQ.Enumeration.Indexing;
using ParquetSharpLINQ.Query;
using ParquetSharpLINQ.Mappers;

namespace ParquetSharpLINQ.Enumeration;

internal static class PropertyColumnMapper<T> where T : new()
{
    public static Dictionary<string, RangeFilter> MapRangeFilterPropertyNamesToColumnNames(
        IReadOnlyDictionary<string, RangeFilter> propertyRangeFilters)
    {
        var mapped = new Dictionary<string, RangeFilter>(StringComparer.OrdinalIgnoreCase);

        foreach (var (propertyName, filter) in propertyRangeFilters)
        {
            var columnName = ResolveColumnNameFromProperty(propertyName);
            mapped[columnName] = filter;
        }

        return mapped;
    }

    private static string ResolveColumnNameFromProperty(string propertyName)
    {
        var map = GetPropertyToColumnMap();
        return map.GetValueOrDefault(propertyName, propertyName);
    }

    public static Dictionary<string, string> BuildPropertyToColumnMap()
    {
        return new Dictionary<string, string>(GetPropertyToColumnMap(), StringComparer.OrdinalIgnoreCase);
    }

    public static IReadOnlyDictionary<string, string> GetPropertyToColumnMap()
    {
        return PropertyColumnMapperCache.GetPropertyToColumnMap(typeof(T));
    }

    public static IImmutableSet<string> GetIndexedColumnNames()
    {
        return PropertyColumnMapperCache.GetIndexedColumnNames(typeof(T));
    }

    public static bool IsIndexedColumnName(string columnName)
    {
        return PropertyColumnMapperCache.GetIndexedColumnNames(typeof(T)).Contains(columnName);
    }

    public static bool TryGetIndexedColumnDefinition(string propertyName, out IIndexedColumnDefinition? definition)
    {
        var definitions = PropertyColumnMapperCache.GetIndexedColumnDefinitions(typeof(T));
        if (definitions.TryGetValue(propertyName, out var found))
        {
            definition = found;
            return true;
        }

        definition = null;
        return false;
    }

    public static bool AreAllColumnsPartitions(IReadOnlyCollection<string> requestedColumns)
    {
        var partitionColumns = PropertyColumnMapperCache.GetPartitionPropertyNames(typeof(T));
        return requestedColumns.All(col => partitionColumns.Contains(col));
    }

    public static IImmutableSet<string> GetPartitionPropertyNames()
    {
        return PropertyColumnMapperCache.GetPartitionPropertyNames(typeof(T));
    }
}

internal static class PropertyColumnMapperCache
{
    private static readonly ConcurrentDictionary<Type, Lazy<IImmutableDictionary<string, string>>> PropertyToColumnMap =
        new();

    private static readonly ConcurrentDictionary<Type, Lazy<IImmutableSet<string>>> PartitionPropertyNames =
        new();

    private static readonly ConcurrentDictionary<Type, Lazy<IImmutableDictionary<string, IIndexedColumnDefinition>>> IndexedColumnDefinitions =
        new();

    private static readonly ConcurrentDictionary<Type, Lazy<IImmutableSet<string>>> IndexedColumnNames =
        new();

    public static IImmutableDictionary<string, string> GetPropertyToColumnMap(Type type)
        => PropertyToColumnMap.GetOrAdd(type, t => new Lazy<IImmutableDictionary<string, string>>(() => BuildPropertyToColumnMapInternal(t))).Value;

    public static IImmutableSet<string> GetPartitionPropertyNames(Type type)
        => PartitionPropertyNames.GetOrAdd(type, t => new Lazy<IImmutableSet<string>>(() => BuildPartitionPropertyNames(t))).Value;

    public static IImmutableDictionary<string, IIndexedColumnDefinition> GetIndexedColumnDefinitions(Type type)
        => IndexedColumnDefinitions.GetOrAdd(type, t => new Lazy<IImmutableDictionary<string, IIndexedColumnDefinition>>(() => BuildIndexedColumnDefinitions(t))).Value;

    public static IImmutableSet<string> GetIndexedColumnNames(Type type)
        => IndexedColumnNames.GetOrAdd(type, t => new Lazy<IImmutableSet<string>>(() =>
            GetIndexedColumnDefinitions(t).Values
                .Select(def => def.ColumnName)
                .ToImmutableHashSet(StringComparer.OrdinalIgnoreCase))).Value;

    private static IImmutableSet<string> BuildPartitionPropertyNames(Type type)
    {
        if (!ParquetMapperRegistry.TryGetMetadata(type, out var metadata) || metadata == null)
        {
            throw new InvalidOperationException(
                $"No generated Parquet metadata found for type {type.FullName}. " +
                "Ensure the type has [ParquetColumn] attributes and the project is built.");
        }

        return metadata.PartitionPropertyNames;
    }

    private static IImmutableDictionary<string, string> BuildPropertyToColumnMapInternal(Type type)
    {
        if (!ParquetMapperRegistry.TryGetMetadata(type, out var metadata) || metadata == null)
        {
            throw new InvalidOperationException(
                $"No generated Parquet metadata found for type {type.FullName}. " +
                "Ensure the type has [ParquetColumn] attributes and the project is built.");
        }

        return metadata.PropertyToColumnMap;
    }

    private static IImmutableDictionary<string, IIndexedColumnDefinition> BuildIndexedColumnDefinitions(Type type)
    {
        if (!ParquetMapperRegistry.TryGetMetadata(type, out var metadata) || metadata == null)
        {
            throw new InvalidOperationException(
                $"No generated Parquet metadata found for type {type.FullName}. " +
                "Ensure the type has [ParquetColumn] attributes and the project is built.");
        }

        return metadata.IndexedColumnDefinitions;
    }
}
