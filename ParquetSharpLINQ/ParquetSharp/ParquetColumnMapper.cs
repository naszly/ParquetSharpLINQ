using ParquetSharp;

namespace ParquetSharpLINQ.ParquetSharp;

/// <summary>
/// Handles mapping between column names and Parquet schema descriptors.
/// </summary>
internal static class ParquetColumnMapper
{
    private static readonly StringComparer ColumnNameComparer = StringComparer.OrdinalIgnoreCase;

    /// <summary>
    /// Builds a map of column names to their handles from a schema.
    /// </summary>
    public static Dictionary<string, ColumnHandle> BuildColumnMap(SchemaDescriptor schema)
    {
        var columnMap = new Dictionary<string, ColumnHandle>(ColumnNameComparer);
        
        for (var i = 0; i < schema.NumColumns; ++i)
        {
            var columnDescriptor = schema.Column(i);
            var path = GetColumnPath(columnDescriptor);
            columnMap.TryAdd(path, new ColumnHandle(i, columnDescriptor));
        }

        return columnMap;
    }

    /// <summary>
    /// Extracts the dot-separated column path from a descriptor.
    /// </summary>
    public static string GetColumnPath(ColumnDescriptor descriptor)
    {
        return descriptor.Path.ToDotString();
    }

    /// <summary>
    /// Prepares the list of columns to read. If no columns specified, returns all available columns.
    /// </summary>
    public static List<string> PrepareRequestedColumns(
        IEnumerable<string> requestedColumns,
        Dictionary<string, ColumnHandle> availableColumns)
    {
        var columns = requestedColumns.ToList();
        if (columns.Count != 0)
        {
            return columns;
        }

        columns.AddRange(availableColumns.Keys);
        return columns;
    }

    /// <summary>
    /// Represents a handle to a column with its index and descriptor.
    /// </summary>
    public record ColumnHandle(int Index, ColumnDescriptor Descriptor);
}

