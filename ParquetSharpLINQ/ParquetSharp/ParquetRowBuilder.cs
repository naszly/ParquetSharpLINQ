using System.Collections.Immutable;
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
        List<string> columnsToRead,
        Dictionary<string, ParquetColumnMapper.ColumnHandle> availableColumns)
    {
        using var rowGroupReader = reader.RowGroup(rowGroupIndex);
        var numRows = checked((int)rowGroupReader.MetaData.NumRows);

        if (numRows == 0)
        {
            yield break;
        }

        var columnNames = columnsToRead.ToArray();

        var columnBuffers = ReadAllColumns(rowGroupReader, columnNames, availableColumns, numRows);

        for (var rowIndex = 0; rowIndex < numRows; rowIndex++)
        {
            yield return BuildRow(columnNames, columnBuffers, rowIndex);
        }
    }

    /// <summary>
    /// Reads all requested columns from a row group into memory buffers.
    /// </summary>
    private static ImmutableArray<object?>[] ReadAllColumns(
        RowGroupReader rowGroupReader,
        string[] columnNames,
        Dictionary<string, ParquetColumnMapper.ColumnHandle> availableColumns,
        int numRows)
    {
        var buffers = new ImmutableArray<object?>[columnNames.Length];

        for (var i = 0; i < columnNames.Length; i++)
        {
            var columnName = columnNames[i];
            var handle = availableColumns[columnName];
            buffers[i] = ParquetColumnReader.ReadColumn(rowGroupReader, handle, numRows);
        }

        return buffers;
    }

    /// <summary>
    /// Constructs a single row by extracting values from column buffers at the specified index.
    /// </summary>
    private static ParquetRow BuildRow(
        string[] columnNames,
        ImmutableArray<object?>[] columnBuffers,
        int rowIndex)
    {
        var values = new object?[columnNames.Length];

        for (var i = 0; i < columnNames.Length; i++)
        {
            values[i] = columnBuffers[i][rowIndex];
        }

        return new ParquetRow(columnNames, values);
    }
}

