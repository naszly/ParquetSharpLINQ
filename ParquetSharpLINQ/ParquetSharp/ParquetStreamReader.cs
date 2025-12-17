using System.Collections.Immutable;
using ParquetSharp;

namespace ParquetSharpLINQ.ParquetSharp;

/// <summary>
/// Provides static helper methods for reading Parquet data from streams.
/// Used by both file-based and Azure blob-based readers.
/// </summary>
public static class ParquetStreamReader
{
    /// <summary>
    /// Reads column metadata from a stream.
    /// </summary>
    public static ImmutableArray<Column> GetColumnsFromStream(Stream? stream)
    {
        if (stream == null)
        {
            return ImmutableArray<Column>.Empty;
        }

        stream.Position = 0;
        using var reader = new ParquetFileReader(stream);
        var schema = reader.FileMetaData.Schema ??
                     throw new InvalidOperationException("Unable to read Parquet schema.");

        var builder = ImmutableArray.CreateBuilder<Column>(schema.NumColumns);
        for (var i = 0; i < schema.NumColumns; i++)
        {
            var descriptor = schema.Column(i);
            builder.Add(new Column(descriptor.LogicalType.GetType(), descriptor.Name));
        }
        
        return builder.ToImmutable();
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

        var columnsToRead = ParquetColumnMapper.GetRequestedColumns(columns, schema).ToArray();

        var numRowGroups = reader.FileMetaData.NumRowGroups;
        for (var rowGroupIndex = 0; rowGroupIndex < numRowGroups; rowGroupIndex++)
        {
            foreach (var row in ParquetRowBuilder.ReadRowGroup(reader, rowGroupIndex, columnsToRead))
            {
                yield return row;
            }
        }
    }
}

