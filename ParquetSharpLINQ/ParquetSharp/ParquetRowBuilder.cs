using System.Collections.Immutable;
using ParquetSharp;

namespace ParquetSharpLINQ.ParquetSharp;

using ParquetRow = Dictionary<string, object?>;

/// <summary>
/// Handles building rows from Parquet row groups.
/// </summary>
internal static class ParquetRowBuilder
{
    private static readonly StringComparer ColumnNameComparer = StringComparer.OrdinalIgnoreCase;

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

        var columnBuffers = ReadAllColumns(rowGroupReader, columnsToRead, availableColumns, numRows);

        for (var rowIndex = 0; rowIndex < numRows; rowIndex++)
        {
            yield return BuildRow(columnsToRead, columnBuffers, rowIndex);
        }
    }

    /// <summary>
    /// Reads all requested columns from a row group into memory buffers.
    /// </summary>
    private static Dictionary<string, ImmutableArray<object?>> ReadAllColumns(
        RowGroupReader rowGroupReader,
        List<string> columnsToRead,
        Dictionary<string, ParquetColumnMapper.ColumnHandle> availableColumns,
        int numRows)
    {
        var buffers = new Dictionary<string, ImmutableArray<object?>>(columnsToRead.Count, ColumnNameComparer);

        foreach (var columnName in columnsToRead)
        {
            var handle = availableColumns[columnName];
            buffers[columnName] = ParquetColumnReader.ReadColumn(rowGroupReader, handle, numRows);
        }

        return buffers;
    }

    /// <summary>
    /// Constructs a single row by extracting values from column buffers at the specified index.
    /// </summary>
    private static ParquetRow BuildRow(
        List<string> columnsToRead,
        Dictionary<string, ImmutableArray<object?>> buffers,
        int rowIndex)
    {
        var row = new ParquetRow(columnsToRead.Count, ColumnNameComparer);

        foreach (var columnName in columnsToRead)
        {
            row[columnName] = buffers[columnName][rowIndex];
        }

        return row;
    }
}

