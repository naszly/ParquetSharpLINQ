using ParquetSharpLINQ.Attributes;
using ParquetSharpLINQ.Query;
using System.Reflection;

namespace ParquetSharpLINQ.Enumeration;

internal static class PropertyColumnMapper<T> where T : new()
{
    public static Dictionary<string, object?> MapPropertyNamesToColumnNames(
        IReadOnlyDictionary<string, object?> propertyFilters)
    {
        var mapped = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);

        foreach (var (propertyName, value) in propertyFilters)
        {
            var columnName = ResolveColumnNameFromProperty(propertyName);
            mapped[columnName] = FilterValueNormalizer.Normalize(value);
        }

        return mapped;
    }

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
        var property = typeof(T).GetProperty(propertyName, BindingFlags.Public | BindingFlags.Instance);
        if (property == null)
        {
            return propertyName;
        }

        var attr = property.GetCustomAttribute<ParquetColumnAttribute>();
        return attr?.Name ?? propertyName;
    }

    public static Dictionary<string, string> BuildPropertyToColumnMap()
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

    public static bool AreAllColumnsPartitions(IReadOnlyCollection<string> requestedColumns)
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

