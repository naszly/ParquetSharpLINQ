using ParquetSharp;
using ParquetSharpLINQ.Interfaces;
using ParquetSharpLINQ.ParquetSharp;

namespace ParquetSharpLINQ.Tests.Integration.Helpers;

/// <summary>
/// Test reader that tracks which files are actually read.
/// Used to verify that range filters skip reading unnecessary files.
/// </summary>
public class TrackingParquetReader : IParquetReader
{
    private readonly ParquetSharpReader _innerReader = new();
    private readonly HashSet<string> _filesRead = new();
    private readonly object _lock = new();

    public IReadOnlySet<string> FilesRead => _filesRead;

    public IEnumerable<Column> GetColumns(string filePath)
    {
        return _innerReader.GetColumns(filePath);
    }

    public IEnumerable<ParquetRow> ReadRows(string filePath, IEnumerable<string> columns)
    {
        // Track that this file was read
        lock (_lock)
        {
            _filesRead.Add(Path.GetFileName(filePath));
        }

        return _innerReader.ReadRows(filePath, columns);
    }
}

