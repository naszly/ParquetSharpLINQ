using ParquetSharpLINQ.Interfaces;

namespace ParquetSharpLINQ.Enumeration;

internal static class ColumnResolver<T> where T : new()
{
    public static IReadOnlyList<string> ResolveColumnsToRead(
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
            normalizedBase = FilterRequestedColumns(normalizedBase, requestedColumns);
        }

        if (normalizedBase.Count == 0)
        {
            return availableColumnNames;
        }

        ValidateRequiredColumnsExist(normalizedBase, availableColumnNames);

        return normalizedBase;
    }

    private static List<string> FilterRequestedColumns(
        List<string> baseColumns,
        IReadOnlyCollection<string> requestedColumns)
    {
        var propertyToColumnMap = PropertyColumnMapper<T>.BuildPropertyToColumnMap();
        var requestedColumnNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        foreach (var propertyName in requestedColumns)
        {
            var columnName = propertyToColumnMap.GetValueOrDefault(propertyName, propertyName);
            requestedColumnNames.Add(columnName);
        }

        return baseColumns
            .Where(col => requestedColumnNames.Contains(col))
            .ToList();
    }

    private static void ValidateRequiredColumnsExist(
        List<string> requiredColumns,
        IReadOnlyList<string> availableColumnNames)
    {
        var availableSet = new HashSet<string>(availableColumnNames, StringComparer.OrdinalIgnoreCase);
        var missing = requiredColumns
            .Where(column => !availableSet.Contains(column))
            .ToList();

        if (missing.Count > 0)
        {
            throw new InvalidOperationException(
                $"Parquet file is missing required columns: {string.Join(", ", missing)}");
        }
    }
}

