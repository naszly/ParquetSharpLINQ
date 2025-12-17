using ParquetSharp;

namespace ParquetSharpLINQ.ParquetSharp;

/// <summary>
/// Handles mapping between column names and Parquet schema descriptors.
/// </summary>
internal static class ParquetColumnMapper
{
    private static readonly StringComparer ColumnNameComparer = StringComparer.OrdinalIgnoreCase;

    /// <summary>
    /// Maps requested column names to their corresponding Parquet schema descriptors.
    /// </summary>
    public static List<ColumnHandle> GetRequestedColumns(
        IEnumerable<string> requestedColumns,
        SchemaDescriptor schema)
    {
        List<ColumnHandle> mappings;

        var columns = requestedColumns as string[] ?? requestedColumns.ToArray();
        
        if (columns.Any())
        {
            mappings = [];
            for (var i = 0; i < schema.NumColumns; i++)
            {
                var columnDescriptor = schema.Column(i);
                var columnName = columnDescriptor.Name;

                if (columns.Contains(columnName, ColumnNameComparer))
                {
                    mappings.Add(new ColumnHandle(i, columnDescriptor));
                }
            }
        }
        else
        {
            mappings = new List<ColumnHandle>(schema.NumColumns);
            for (var i = 0; i < schema.NumColumns; i++)
            {
                var columnDescriptor = schema.Column(i);
                mappings.Add(new ColumnHandle(i, columnDescriptor));
            }
        }
        
        return mappings;
    }

    /// <summary>
    /// Represents a handle to a column with its index and descriptor.
    /// </summary>
    public record ColumnHandle(int Index, ColumnDescriptor Descriptor);
}

