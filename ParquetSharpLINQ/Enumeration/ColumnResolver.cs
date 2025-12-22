namespace ParquetSharpLINQ.Enumeration;

internal static class ColumnResolver<T> where T : new()
{
    public static IReadOnlyList<string> ResolveColumnsToRead(
        IReadOnlyList<string> availableColumnNames,
        IReadOnlyCollection<string>? columnsToRead)
    {
        if (columnsToRead == null || columnsToRead.Count == 0)
            return availableColumnNames;

        var resolvedColumns = MapRequestedColumns(columnsToRead)
            .Where(name => !string.IsNullOrWhiteSpace(name))
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToList();

        if (resolvedColumns.Count == 0)
            return [];

        ValidateColumnsExist(resolvedColumns, availableColumnNames);

        return resolvedColumns;
    }

    private static HashSet<string> MapRequestedColumns(IReadOnlyCollection<string> requestedColumns)
    {
        var propertyToColumnMap = PropertyColumnMapper<T>.BuildPropertyToColumnMap();
        var partitionPropertyNames = PropertyColumnMapper<T>.GetPartitionPropertyNames();
        var requestedColumnNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        foreach (var propertyName in requestedColumns)
        {
            if (partitionPropertyNames.Contains(propertyName))
                continue;
            var columnName = propertyToColumnMap.GetValueOrDefault(propertyName, propertyName);
            requestedColumnNames.Add(columnName);
        }

        return requestedColumnNames;
    }

    private static void ValidateColumnsExist(
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
