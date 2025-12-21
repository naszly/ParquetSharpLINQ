using System.Collections.Concurrent;
using System.Collections.Immutable;
using System.Reflection;
using ParquetSharpLINQ.Attributes;
using ParquetSharpLINQ.Enumeration.Indexing;
using ParquetSharpLINQ.Query;

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

    public static bool TryGetIndexedColumnDefinition(string propertyName, out IndexedColumnDefinition? definition)
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

    private static readonly ConcurrentDictionary<Type, Lazy<IImmutableDictionary<string, IndexedColumnDefinition>>> IndexedColumnDefinitions =
        new();

    private static readonly ConcurrentDictionary<Type, Lazy<IImmutableSet<string>>> IndexedColumnNames =
        new();

    public static IImmutableDictionary<string, string> GetPropertyToColumnMap(Type type)
        => PropertyToColumnMap.GetOrAdd(type, t => new Lazy<IImmutableDictionary<string, string>>(() => BuildPropertyToColumnMapInternal(t))).Value;

    public static IImmutableSet<string> GetPartitionPropertyNames(Type type)
        => PartitionPropertyNames.GetOrAdd(type, t => new Lazy<IImmutableSet<string>>(() => BuildPartitionPropertyNames(t))).Value;

    public static IImmutableDictionary<string, IndexedColumnDefinition> GetIndexedColumnDefinitions(Type type)
        => IndexedColumnDefinitions.GetOrAdd(type, t => new Lazy<IImmutableDictionary<string, IndexedColumnDefinition>>(() => BuildIndexedColumnDefinitions(t))).Value;

    public static IImmutableSet<string> GetIndexedColumnNames(Type type)
        => IndexedColumnNames.GetOrAdd(type, t => new Lazy<IImmutableSet<string>>(() =>
            GetIndexedColumnDefinitions(t).Values
                .Select(def => def.ColumnName)
                .ToImmutableHashSet(StringComparer.OrdinalIgnoreCase))).Value;

    private static ImmutableHashSet<string> BuildPartitionPropertyNames(Type type)
    {
        var properties = type.GetProperties(BindingFlags.Instance | BindingFlags.Public);
        var partitionColumns = ImmutableHashSet.CreateBuilder<string>(StringComparer.OrdinalIgnoreCase);

        foreach (var property in properties)
        {
            var attr = property.GetCustomAttribute<ParquetColumnAttribute>();
            if (attr?.IsPartition == true)
            {
                partitionColumns.Add(property.Name);
            }
        }

        return partitionColumns.ToImmutable();
    }

    private static ImmutableDictionary<string, string> BuildPropertyToColumnMapInternal(Type type)
    {
        var builder = ImmutableDictionary.CreateBuilder<string, string>(StringComparer.OrdinalIgnoreCase);
        var properties = type.GetProperties(BindingFlags.Instance | BindingFlags.Public);

        foreach (var property in properties)
        {
            var attr = property.GetCustomAttribute<ParquetColumnAttribute>();
            if (attr != null)
            {
                var columnName = string.IsNullOrWhiteSpace(attr.Name) ? property.Name : attr.Name;
                builder[property.Name] = columnName;
            }
        }

        return builder.ToImmutable();
    }

    private static ImmutableDictionary<string, IndexedColumnDefinition> BuildIndexedColumnDefinitions(Type type)
    {
        var builder = ImmutableDictionary.CreateBuilder<string, IndexedColumnDefinition>(StringComparer.OrdinalIgnoreCase);
        var properties = type.GetProperties(BindingFlags.Instance | BindingFlags.Public);

        foreach (var property in properties)
        {
            var attr = property.GetCustomAttribute<ParquetColumnAttribute>();
            if (attr?.Indexed != true)
                continue;

            var definition = IndexedColumnDefinition.Create(property);
            builder[property.Name] = definition;
        }

        return builder.ToImmutable();
    }
}
