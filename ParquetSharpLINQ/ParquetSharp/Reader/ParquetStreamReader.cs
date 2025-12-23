using System.Collections.Immutable;
using ParquetSharp;
using ParquetSharpLINQ.ParquetSharp.ParquetRow;

namespace ParquetSharpLINQ.ParquetSharp.Reader;

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
    public static IEnumerable<ParquetRow.ParquetRow> ReadRowsFromStream(
        Stream? stream,
        IEnumerable<string> columns,
        IReadOnlySet<int>? rowGroupsToRead = null)
    {
        if (stream == null)
        {
            yield break;
        }

        stream.Position = 0;
        using var reader = new ParquetFileReader(stream);
        var schema = reader.FileMetaData.Schema ??
                     throw new InvalidOperationException("Unable to read Parquet schema.");

        var columnsToRead = ParquetColumnResolver.ResolveRequestedColumns(columns, schema).ToArray();

        var numRowGroups = reader.FileMetaData.NumRowGroups;
        for (var rowGroupIndex = 0; rowGroupIndex < numRowGroups; rowGroupIndex++)
        {
            if (rowGroupsToRead != null && !rowGroupsToRead.Contains(rowGroupIndex))
                continue;

            foreach (var row in ParquetRowBuilder.ReadRowGroup(reader, rowGroupIndex, columnsToRead))
            {
                yield return row;
            }
        }
    }

    public static ImmutableArray<ImmutableArray<T>> ReadColumnValuesByRowGroupFromStream<T>(
        Stream? stream,
        string columnName)
    {
        if (stream == null)
        {
            return ImmutableArray<ImmutableArray<T>>.Empty;
        }

        stream.Position = 0;
        using var reader = new ParquetFileReader(stream);
        var schema = reader.FileMetaData.Schema ??
                     throw new InvalidOperationException("Unable to read Parquet schema.");

        var handles = ParquetColumnResolver.ResolveRequestedColumns(new[] { columnName }, schema);
        if (handles.Count == 0)
        {
            throw new InvalidOperationException($"Column '{columnName}' not found in Parquet schema.");
        }

        var handle = handles[0];

        var rowGroupCount = reader.FileMetaData.NumRowGroups;
        var builder = ImmutableArray.CreateBuilder<ImmutableArray<T>>(rowGroupCount);

        for (var rowGroupIndex = 0; rowGroupIndex < rowGroupCount; rowGroupIndex++)
        {
            using var rowGroup = reader.RowGroup(rowGroupIndex);
            var numRows = checked((int)rowGroup.MetaData.NumRows);
            var values = ParquetColumnReader.ReadColumn<T>(rowGroup, handle, numRows);
            builder.Add(values);
        }

        return builder.ToImmutable();
    }
}
