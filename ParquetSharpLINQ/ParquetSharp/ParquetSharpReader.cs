using System.Collections.Immutable;
using ParquetSharp;
using ParquetSharpLINQ.Interfaces;

namespace ParquetSharpLINQ.ParquetSharp;

/// <summary>
/// File-based Parquet reader implementation.
/// Delegates to specialized classes for schema mapping, type resolution, and data reading.
/// </summary>
public class ParquetSharpReader : IParquetReader
{
    public IEnumerable<string> ListFiles(string directory)
    {
        ValidateDirectory(directory);
        return Directory.EnumerateFiles(directory, "*.parquet", SearchOption.AllDirectories);
    }

    public IEnumerable<Column> GetColumns(string filePath)
    {
        ValidateFilePath(filePath);
        using var stream = File.OpenRead(filePath);
        foreach (var column in ParquetStreamReader.GetColumnsFromStream(stream))
        {
            yield return column;
        }
    }

    public IEnumerable<ParquetRow> ReadRows(
        string filePath,
        IEnumerable<string> columns,
        IReadOnlySet<int>? rowGroupsToRead)
    {
        ValidateFilePath(filePath);
        using var stream = File.OpenRead(filePath);
        foreach (var row in ParquetStreamReader.ReadRowsFromStream(stream, columns, rowGroupsToRead))
        {
            yield return row;
        }
    }

    public IReadOnlyList<ImmutableArray<T>> ReadColumnValuesByRowGroup<T>(string filePath, string columnName)
    {
        ValidateFilePath(filePath);
        using var stream = File.OpenRead(filePath);
        return ParquetStreamReader.ReadColumnValuesByRowGroupFromStream<T>(stream, columnName);
    }

    private static void ValidateDirectory(string directory)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(directory);

        if (!Directory.Exists(directory))
        {
            throw new DirectoryNotFoundException($"Directory not found: {directory}");
        }
    }

    private static void ValidateFilePath(string filePath)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(filePath);

        if (!File.Exists(filePath))
        {
            throw new FileNotFoundException($"Parquet file not found: {filePath}", filePath);
        }
    }
}
