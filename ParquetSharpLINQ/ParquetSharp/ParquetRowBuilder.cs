using ParquetSharp;

namespace ParquetSharpLINQ.ParquetSharp;

/// <summary>
/// Handles building rows from Parquet row groups.
/// </summary>
internal static class ParquetRowBuilder
{
    /// <summary>
    /// Reads all rows from a specific row group.
    /// </summary>
    public static IEnumerable<ParquetRow> ReadRowGroup(
        ParquetFileReader reader,
        int rowGroupIndex,
        ParquetColumnMapper.ColumnHandle[] columnsToRead)
    {
        using var rowGroupReader = reader.RowGroup(rowGroupIndex);
        var numRows = checked((int)rowGroupReader.MetaData.NumRows);

        if (numRows == 0)
        {
            yield break;
        }

        var columnNames = columnsToRead.Select(x => x.Descriptor.Name).ToArray();

        var columnBuffers = ReadAllColumns(rowGroupReader, columnsToRead, numRows);

        for (var rowIndex = 0; rowIndex < numRows; rowIndex++)
        {
            yield return BuildRow(columnNames, columnBuffers, rowIndex);
        }
    }

    /// <summary>
    /// Reads all requested columns from a row group into memory buffers.
    /// </summary>
    private static IColumnBuffer[] ReadAllColumns(
        RowGroupReader rowGroupReader,
        ParquetColumnMapper.ColumnHandle[] columnHandles,
        int numRows)
    {
        var buffers = new IColumnBuffer[columnHandles.Length];

        for (var i = 0; i < columnHandles.Length; i++)
        {
            var handle = columnHandles[i];
            var targetType = ParquetTypeResolver.ResolveClrType(handle.Descriptor);
            if (ParquetTypeResolver.ShouldUseNullable(handle.Descriptor))
            {
                targetType = ParquetTypeResolver.ToNullableType(targetType);
            }

            buffers[i] = ParquetColumnReader.ReadColumnBuffer(rowGroupReader, handle, numRows, targetType);
        }

        return buffers;
    }

    /// <summary>
    /// Constructs a single row by extracting values from column buffers at the specified index.
    /// </summary>
    private static ParquetRow BuildRow(
        string[] columnNames,
        IColumnBuffer[] columnBuffers,
        int rowIndex)
    {
        return new ParquetRow(columnNames, columnBuffers, rowIndex);
    }
}
