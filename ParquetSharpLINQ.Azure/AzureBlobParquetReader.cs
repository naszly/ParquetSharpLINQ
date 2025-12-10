using Azure.Storage.Blobs;
using ParquetSharp;
using ParquetSharpLINQ.ParquetSharp;

namespace ParquetSharpLINQ.Azure;

#if !NET9_0_OR_GREATER
using Lock = System.Object;
#endif

/// <summary>
/// Parquet reader that streams files from Azure Blob Storage without downloading to disk.
/// Uses in-memory caching with LRU eviction for performance optimization.
/// Delegates actual Parquet reading to ParquetStreamReader.
/// </summary>
public sealed class AzureBlobParquetReader : IAsyncParquetReader, IDisposable
{
    private readonly Lock _streamCacheLock = new();
    private readonly BlobContainerClient _containerClient;
    private readonly Dictionary<string, byte[]> _streamCache = new(StringComparer.OrdinalIgnoreCase);
    private readonly LinkedList<string> _cacheAccessOrder = new();
    private readonly Dictionary<string, LinkedListNode<string>> _cacheNodes = new(StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<string, SemaphoreSlim> _downloadLocks = new(StringComparer.OrdinalIgnoreCase);
    
    private readonly long _maxCacheSizeBytes;
    private long _currentCacheSizeBytes;

    private const int TaskCleanupThresholdMultiplier = 2;

    public AzureBlobParquetReader(
        string connectionString, 
        string containerName,
        long maxCacheSizeBytes = ParquetConfiguration.DefaultMaxCacheSizeBytes)
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
        long maxCacheSizeBytes = ParquetConfiguration.DefaultMaxCacheSizeBytes)
    {
        _containerClient = containerClient;
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

        var downloadLock = GetOrCreateDownloadLock(blobPath);

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
    /// Checks if a blob is already cached without updating LRU order.
    /// Used for fast-path checks in prefetch scenarios.
    /// </summary>
    private bool IsCached(string blobPath)
    {
        lock (_streamCacheLock)
        {
            return _streamCache.ContainsKey(blobPath);
        }
    }

    /// <summary>
    /// Gets or creates a download lock for the specified blob path.
    /// Ensures only one download happens per blob at a time.
    /// </summary>
    private SemaphoreSlim GetOrCreateDownloadLock(string blobPath)
    {
        lock (_streamCacheLock)
        {
            if (!_downloadLocks.TryGetValue(blobPath, out var downloadLock))
            {
                downloadLock = new SemaphoreSlim(1, 1);
                _downloadLocks[blobPath] = downloadLock;
            }
            return downloadLock;
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
    /// Asynchronously downloads the specified blob to a byte array.
    /// </summary>
    /// <returns>A byte array containing the blob data, or null if the blob doesn't exist.</returns>
    private async Task<byte[]?> DownloadBlobToMemoryAsync(string blobPath)
    {
        var blobClient = _containerClient.GetBlobClient(blobPath);

        var exists = await blobClient.ExistsAsync();
        if (!exists)
        {
            return null;
        }

        using var memoryStream = new MemoryStream();
        await blobClient.DownloadToAsync(memoryStream);
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
    /// Prefetches multiple blobs into cache in parallel.
    /// This is useful for warming the cache before enumeration.
    /// Uses a producer-consumer pattern to limit concurrent task creation and memory usage.
    /// </summary>
    public async Task PrefetchAsync(IEnumerable<string> filePaths, int maxParallelism = ParquetConfiguration.DefaultPrefetchParallelism)
    {
        ArgumentNullException.ThrowIfNull(filePaths);

        using var semaphore = new SemaphoreSlim(maxParallelism, maxParallelism);
        var activeTasks = new Queue<Task>(maxParallelism * TaskCleanupThresholdMultiplier);
        
        foreach (var blobPath in filePaths)
        {
            await semaphore.WaitAsync();
            
            var task = PrefetchWithSemaphoreAsync(blobPath, semaphore);
            activeTasks.Enqueue(task);
            
            if (activeTasks.Count >= maxParallelism * TaskCleanupThresholdMultiplier)
            {
                while (activeTasks.Count > 0 && activeTasks.Peek().IsCompleted)
                {
                    _ = activeTasks.Dequeue();
                }
            }
        }
        
        // Wait for all remaining tasks to complete
        await Task.WhenAll(activeTasks);
    }

    /// <summary>
    /// Helper method to prefetch a blob with semaphore-based throttling.
    /// </summary>
    private async Task PrefetchWithSemaphoreAsync(string blobPath, SemaphoreSlim semaphore)
    {
        try
        {
            await PrefetchBlobAsync(blobPath);
        }
        finally
        {
            semaphore.Release();
        }
    }

    /// <summary>
    /// Downloads and caches a single blob without creating a MemoryStream wrapper.
    /// Optimized for prefetch scenarios where the stream object is not needed.
    /// </summary>
    private async Task PrefetchBlobAsync(string blobPath)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(blobPath);

        // Fast path: check if already cached
        if (IsCached(blobPath))
        {
            return;
        }

        var downloadLock = GetOrCreateDownloadLock(blobPath);

        // Acquire blob-specific lock to prevent concurrent downloads
        await downloadLock.WaitAsync();
        try
        {
            // Double-check cache after acquiring lock
            if (IsCached(blobPath))
            {
                return;
            }

            // Download and cache (no stream creation)
            var downloadedData = await DownloadBlobToMemoryAsync(blobPath);
            if (downloadedData != null)
            {
                CacheData(blobPath, downloadedData);
            }
        }
        finally
        {
            downloadLock.Release();
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