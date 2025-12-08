using ParquetSharp;

namespace ParquetSharpLINQ.ParquetSharp;

using ParquetRow = Dictionary<string, object?>;

/// <summary>
/// Provides static helper methods for reading Parquet data from streams.
/// Used by both file-based and Azure blob-based readers.
/// </summary>
public static class ParquetStreamReader
{
    /// <summary>
    /// Reads column metadata from a stream.
    /// </summary>
    public static IEnumerable<Column> GetColumnsFromStream(Stream? stream)
    {
        if (stream == null)
        {
            yield break;
        }

        stream.Position = 0;
        using var reader = new ParquetFileReader(stream);
        var schema = reader.FileMetaData.Schema ??
                     throw new InvalidOperationException("Unable to read Parquet schema.");

        for (var i = 0; i < schema.NumColumns; i++)
        {
            var descriptor = schema.Column(i);
            yield return new Column(descriptor.LogicalType.GetType(), ParquetColumnMapper.GetColumnPath(descriptor));
        }
    }

    /// <summary>
    /// Reads rows from a stream with optional column filtering.
    /// </summary>
    public static IEnumerable<ParquetRow> ReadRowsFromStream(Stream? stream, IEnumerable<string> columns)
    {
        if (stream == null)
        {
            yield break;
        }

        stream.Position = 0;
        using var reader = new ParquetFileReader(stream);
        var schema = reader.FileMetaData.Schema ??
                     throw new InvalidOperationException("Unable to read Parquet schema.");

        var availableColumns = ParquetColumnMapper.BuildColumnMap(schema);
        var columnsToRead = ParquetColumnMapper.PrepareRequestedColumns(columns, availableColumns);

        for (var rowGroupIndex = 0; rowGroupIndex < reader.FileMetaData.NumRowGroups; rowGroupIndex++)
        {
            foreach (var row in ParquetRowBuilder.ReadRowGroup(reader, rowGroupIndex, columnsToRead, availableColumns))
            {
                yield return row;
            }
        }
    }
}

