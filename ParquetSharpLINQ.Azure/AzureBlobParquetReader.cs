using Azure.Storage.Blobs;
using ParquetSharp;
using ParquetSharpLINQ.Common;
using ParquetSharpLINQ.ParquetSharp;

namespace ParquetSharpLINQ.Azure;

/// <summary>
/// Parquet reader that streams files from Azure Blob Storage without downloading to disk.
/// Uses in-memory caching with LRU eviction for performance optimization.
/// Delegates actual Parquet reading to ParquetStreamReader.
/// </summary>
public sealed class AzureBlobParquetReader : IParquetReader, IDisposable
{
#if NET9_0_OR_GREATER
    private readonly Lock _streamCacheLock = new();
#else
    private readonly object _streamCacheLock = new();
#endif
    private readonly BlobContainerClient _containerClient;
    private readonly Dictionary<string, byte[]> _streamCache = new(StringComparer.OrdinalIgnoreCase);
    private readonly LinkedList<string> _cacheAccessOrder = new();
    private readonly Dictionary<string, LinkedListNode<string>> _cacheNodes = new(StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<string, SemaphoreSlim> _downloadLocks = new(StringComparer.OrdinalIgnoreCase);
    
    private readonly long _maxCacheSizeBytes;
    private long _currentCacheSizeBytes;

    /// <summary>
    /// Default maximum cache size: 1 GB
    /// </summary>
    private const long DefaultMaxCacheSizeBytes = 1_073_741_824;

    public AzureBlobParquetReader(
        string connectionString, 
        string containerName,
        long maxCacheSizeBytes = DefaultMaxCacheSizeBytes)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(connectionString);
        ArgumentException.ThrowIfNullOrWhiteSpace(containerName);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(maxCacheSizeBytes);

        var serviceClient = new BlobServiceClient(connectionString);
        _containerClient = serviceClient.GetBlobContainerClient(containerName);
        _maxCacheSizeBytes = maxCacheSizeBytes;
    }

    public AzureBlobParquetReader(
        BlobContainerClient containerClient,
        long maxCacheSizeBytes = DefaultMaxCacheSizeBytes)
    {
        _containerClient = containerClient ?? throw new ArgumentNullException(nameof(containerClient));
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(maxCacheSizeBytes);
        
        _maxCacheSizeBytes = maxCacheSizeBytes;
    }

    public void Dispose()
    {
        lock (_streamCacheLock)
        {
            _streamCache.Clear();
            _cacheAccessOrder.Clear();
            _cacheNodes.Clear();
            _currentCacheSizeBytes = 0;

            foreach (var semaphore in _downloadLocks.Values)
            {
                semaphore.Dispose();
            }
            _downloadLocks.Clear();
        }
    }

    public IEnumerable<string> ListFiles(string path)
    {
        ArgumentNullException.ThrowIfNull(path);

        var blobs = _containerClient.GetBlobs(prefix: path);

        foreach (var blob in blobs)
        {
            if (HivePartitionParser.IsParquetFile(blob.Name))
            {
                yield return blob.Name;
            }
        }
    }

    /// <summary>
    /// Opens a stream to the specified blob path. Returns a cached copy if available.
    /// Uses double-checked locking to prevent duplicate downloads.
    /// </summary>
    /// <param name="blobPath">The path to the blob within the container.</param>
    /// <returns>A memory stream containing the blob data, or null if the blob doesn't exist.</returns>
    private MemoryStream? OpenStream(string blobPath)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(blobPath);

        // Fast path: check cache without full lock
        var cachedData = TryGetCachedData(blobPath);
        if (cachedData != null)
        {
            return CreateStreamFromBytes(cachedData);
        }

        // Get or create download lock for this specific blob
        SemaphoreSlim downloadLock;
        lock (_streamCacheLock)
        {
            if (!_downloadLocks.TryGetValue(blobPath, out downloadLock!))
            {
                downloadLock = new SemaphoreSlim(1, 1);
                _downloadLocks[blobPath] = downloadLock;
            }
        }

        // Acquire blob-specific lock to prevent concurrent downloads
        downloadLock.Wait();
        try
        {
            // Double-check cache after acquiring lock
            cachedData = TryGetCachedData(blobPath);
            if (cachedData != null)
            {
                return CreateStreamFromBytes(cachedData);
            }

            // Download and cache
            var downloadedData = DownloadBlobToMemory(blobPath);
            if (downloadedData == null)
            {
                return null;
            }

            CacheData(blobPath, downloadedData);
            return CreateStreamFromBytes(downloadedData);
        }
        finally
        {
            downloadLock.Release();
        }
    }

    /// <summary>
    /// Attempts to retrieve cached data for the specified blob path and updates LRU order.
    /// </summary>
    /// <returns>The cached byte array if found; otherwise, null.</returns>
    private byte[]? TryGetCachedData(string blobPath)
    {
        lock (_streamCacheLock)
        {
            if (!_streamCache.TryGetValue(blobPath, out var cached))
            {
                return null;
            }

            // Update LRU: move to end (most recently used)
            if (_cacheNodes.TryGetValue(blobPath, out var node))
            {
                _cacheAccessOrder.Remove(node);
                _cacheAccessOrder.AddLast(node);
            }

            return cached;
        }
    }

    /// <summary>
    /// Downloads the specified blob to a byte array.
    /// </summary>
    /// <returns>A byte array containing the blob data, or null if the blob doesn't exist.</returns>
    private byte[]? DownloadBlobToMemory(string blobPath)
    {
        var blobClient = _containerClient.GetBlobClient(blobPath);

        if (!blobClient.Exists())
        {
            return null;
        }

        using var memoryStream = new MemoryStream();
        blobClient.DownloadTo(memoryStream);
        return memoryStream.ToArray();
    }

    /// <summary>
    /// Adds data to the cache with LRU eviction if needed.
    /// Ensures the cache doesn't exceed the maximum size by evicting least recently used items.
    /// </summary>
    private void CacheData(string blobPath, byte[] data)
    {
        lock (_streamCacheLock)
        {
            // If already cached (race condition), don't cache again
            if (_streamCache.ContainsKey(blobPath))
            {
                return;
            }

            var dataSize = data.Length;

            // Don't cache if the single item is larger than max cache size
            if (dataSize > _maxCacheSizeBytes)
            {
                return;
            }

            // Evict least recently used items until we have space
            while (_currentCacheSizeBytes + dataSize > _maxCacheSizeBytes && _cacheAccessOrder.Count > 0)
            {
                var oldestKey = _cacheAccessOrder.First!.Value;
                EvictCacheItem(oldestKey);
            }

            // Add new item to cache
            _streamCache[blobPath] = data;
            var node = _cacheAccessOrder.AddLast(blobPath);
            _cacheNodes[blobPath] = node;
            _currentCacheSizeBytes += dataSize;
        }
    }

    /// <summary>
    /// Evicts a single item from the cache.
    /// </summary>
    private void EvictCacheItem(string blobPath)
    {
        if (_streamCache.TryGetValue(blobPath, out var data))
        {
            _currentCacheSizeBytes -= data.Length;
            _streamCache.Remove(blobPath);
        }

        if (_cacheNodes.TryGetValue(blobPath, out var node))
        {
            _cacheAccessOrder.Remove(node);
            _cacheNodes.Remove(blobPath);
        }
    }

    /// <summary>
    /// Creates a read-only memory stream from a byte array without copying.
    /// The stream wraps the array directly for better performance.
    /// </summary>
    private static MemoryStream CreateStreamFromBytes(byte[] data)
    {
        return new MemoryStream(data, writable: false);
    }

    public IEnumerable<Column> GetColumns(string filePath)
    {
        using var stream = OpenAndValidateStream(filePath);
        foreach (var column in ParquetStreamReader.GetColumnsFromStream(stream))
        {
            yield return column;
        }
    }

    public IEnumerable<Dictionary<string, object?>> ReadRows(string filePath, IEnumerable<string> columns)
    {
        ArgumentNullException.ThrowIfNull(columns);

        using var stream = OpenAndValidateStream(filePath);
        foreach (var row in ParquetStreamReader.ReadRowsFromStream(stream, columns))
        {
            yield return row;
        }
    }

    /// <summary>
    /// Opens a stream for the specified file path and validates that it exists.
    /// </summary>
    /// <exception cref="FileNotFoundException">Thrown when the blob is not found.</exception>
    private MemoryStream OpenAndValidateStream(string filePath)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(filePath);

        var stream = OpenStream(filePath);
        return stream ?? throw new FileNotFoundException($"Blob not found: {filePath}", filePath);
    }
}