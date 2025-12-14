using Azure.Storage.Blobs;
using ParquetSharp;
using ParquetSharpLINQ.Azure;
using ParquetSharpLINQ.Interfaces;
using ParquetSharpLINQ.ParquetSharp;

namespace ParquetSharpLINQ.Tests.Integration;

/// <summary>
/// Test reader that tracks which Azure blobs are actually read.
/// Used to verify that range filters skip reading unnecessary blobs.
/// </summary>
public class TrackingAzureBlobReader : IAsyncParquetReader
{
    private readonly AzureBlobParquetReader _innerReader;
    private readonly HashSet<string> _filesRead = new();
    private readonly object _lock = new();

    public IReadOnlySet<string> FilesRead => _filesRead;

    public TrackingAzureBlobReader(BlobContainerClient containerClient)
    {
        _innerReader = new AzureBlobParquetReader(containerClient);
    }

    public IEnumerable<Column> GetColumns(string filePath)
    {
        return _innerReader.GetColumns(filePath);
    }

    public IEnumerable<ParquetRow> ReadRows(string filePath, IEnumerable<string> columns)
    {
        lock (_lock)
        {
            _filesRead.Add(Path.GetFileName(filePath));
        }

        foreach (var row in _innerReader.ReadRows(filePath, columns))
        {
            yield return row;
        }
    }

    public Task PrefetchAsync(IEnumerable<string> filePaths, int maxParallelism = ParquetConfiguration.DefaultPrefetchParallelism)
    {
        return _innerReader.PrefetchAsync(filePaths, maxParallelism);
    }
}

