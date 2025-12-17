using Azure;
using Azure.Storage.Blobs;
using ParquetSharp;
using ParquetSharpLINQ.Interfaces;
using ParquetSharpLINQ.ParquetSharp;

namespace ParquetSharpLINQ.Azure;

#if !NET9_0_OR_GREATER
using Lock = System.Object;
#endif

/// <summary>
/// Parquet reader that downloads files from Azure Blob Storage to a local temp directory.
/// Uses file-based caching with LRU eviction for performance optimization.
/// Delegates actual Parquet reading to ParquetStreamReader.
/// Accepts a pre-configured BlobContainerClient for maximum flexibility and performance tuning.
/// </summary>
public sealed class AzureBlobParquetReader : IParquetReader
{
    private readonly BlobContainerClient _containerClient;

    public AzureBlobParquetReader(
        BlobContainerClient containerClient, long maxCacheSizeBytes = ParquetConfiguration.DefaultMaxCacheSizeBytes)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(maxCacheSizeBytes);
        
        _containerClient = containerClient;
    }

    public IEnumerable<Column> GetColumns(string filePath)
    {
        using var stream = OpenStream(filePath);
        foreach (var column in ParquetStreamReader.GetColumnsFromStream(stream))
        {
            yield return column;
        }
    }

    public IEnumerable<ParquetRow> ReadRows(string filePath, IEnumerable<string> columns)
    {
        ArgumentNullException.ThrowIfNull(columns);

        using var stream = OpenStream(filePath);
        foreach (var row in ParquetStreamReader.ReadRowsFromStream(stream, columns))
        {
            yield return row;
        }
    }
    
    private Stream? OpenStream(string blobPath)
    {
        var blobClient = _containerClient.GetBlobClient(blobPath);
        return blobClient.OpenRead();
    }
}