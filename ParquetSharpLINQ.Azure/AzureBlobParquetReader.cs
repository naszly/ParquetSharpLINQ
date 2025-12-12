using Azure;
using Azure.Core;
using Azure.Core.Pipeline;
using Azure.Storage.Blobs;
using ParquetSharp;
using ParquetSharpLINQ.ParquetSharp;

namespace ParquetSharpLINQ.Azure;

#if !NET9_0_OR_GREATER
using Lock = System.Object;
#endif

/// <summary>
/// Parquet reader that downloads files from Azure Blob Storage to a local temp directory.
/// Uses file-based caching with LRU eviction for performance optimization.
/// Delegates actual Parquet reading to ParquetStreamReader.
/// </summary>
public sealed class AzureBlobParquetReader : IAsyncParquetReader, IDisposable
{
    private readonly Lock _cacheLock = new();
    private readonly BlobContainerClient _containerClient;
    private readonly Dictionary<string, string> _filePathCache = new(StringComparer.OrdinalIgnoreCase);
    private readonly LinkedList<string> _lruOrder = new();
    private readonly Dictionary<string, LinkedListNode<string>> _lruNodes = new(StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<string, SemaphoreSlim> _downloadLocks = new(StringComparer.OrdinalIgnoreCase);
    private readonly SocketsHttpHandler? _socketsHttpHandler;
    private readonly HttpClient? _httpClient;
    private readonly string _tempDirectory;
    
    private readonly long _maxCacheSizeBytes;
    private long _currentCacheSizeBytes;

    public AzureBlobParquetReader(
        string connectionString, 
        string containerName,
        long maxCacheSizeBytes = ParquetConfiguration.DefaultMaxCacheSizeBytes)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(connectionString);
        ArgumentException.ThrowIfNullOrWhiteSpace(containerName);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(maxCacheSizeBytes);
        
        _socketsHttpHandler = new SocketsHttpHandler
        {
            PooledConnectionLifetime = TimeSpan.FromMinutes(10),
            PooledConnectionIdleTimeout = TimeSpan.FromMinutes(5),
            MaxConnectionsPerServer = 100,
            EnableMultipleHttp2Connections = true,
            ConnectTimeout = TimeSpan.FromSeconds(30),
            ResponseDrainTimeout = TimeSpan.FromSeconds(2)
        };

        _httpClient = new HttpClient(_socketsHttpHandler)
        {
            Timeout = TimeSpan.FromMinutes(10),
            DefaultRequestVersion = new Version(2, 0)
        };

        var blobClientOptions = new BlobClientOptions
        {
            Transport = new HttpClientTransport(_httpClient),
            Retry =
            {
                MaxRetries = 3,
                Delay = TimeSpan.FromSeconds(1),
                MaxDelay = TimeSpan.FromSeconds(10),
                Mode = RetryMode.Exponential,
                NetworkTimeout = TimeSpan.FromSeconds(100)
            },
            Diagnostics = { IsLoggingEnabled = false }
        };

        
        var serviceClient = new BlobServiceClient(connectionString, blobClientOptions);
        _containerClient = serviceClient.GetBlobContainerClient(containerName);
        _maxCacheSizeBytes = maxCacheSizeBytes;

        _tempDirectory = Path.Combine(Path.GetTempPath(), "ParquetSharpLINQ", Guid.NewGuid().ToString());
        Directory.CreateDirectory(_tempDirectory);
    }

    public AzureBlobParquetReader(
        BlobContainerClient containerClient,
        long maxCacheSizeBytes = ParquetConfiguration.DefaultMaxCacheSizeBytes)
    {
        _containerClient = containerClient;
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(maxCacheSizeBytes);
        
        _maxCacheSizeBytes = maxCacheSizeBytes;

        _tempDirectory = Path.Combine(Path.GetTempPath(), "ParquetSharpLINQ", Guid.NewGuid().ToString());
        Directory.CreateDirectory(_tempDirectory);
    }

    public void Dispose()
    {
        lock (_cacheLock)
        {
            _filePathCache.Clear();
            _lruOrder.Clear();
            _lruNodes.Clear();
            _currentCacheSizeBytes = 0;

            foreach (var semaphore in _downloadLocks.Values)
            {
                semaphore.Dispose();
            }
            _downloadLocks.Clear();
        }

        _httpClient?.Dispose();
        _socketsHttpHandler?.Dispose();

        try
        {
            Directory.Delete(_tempDirectory, true);
        }
        catch
        {
            // Ignore errors during cleanup
        }
    }

    /// <summary>
    /// Opens a stream to the specified blob path. Returns a cached file stream if available.
    /// Uses double-checked locking to prevent duplicate downloads.
    /// </summary>
    /// <param name="blobPath">The path to the blob within the container.</param>
    /// <returns>A read-only FileStream for the blob, or null if the blob doesn't exist.</returns>
    private FileStream? OpenStream(string blobPath)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(blobPath);

        var cachedPath = TryGetCachedFilePath(blobPath);
        if (cachedPath != null && File.Exists(cachedPath))
        {
            return CreateSequentialReadStream(cachedPath);
        }

        var downloadLock = GetOrCreateDownloadLock(blobPath);

        // Acquire blob-specific lock to prevent concurrent downloads
        downloadLock.Wait();
        try
        {
            // Double-check cache after acquiring lock
            cachedPath = TryGetCachedFilePath(blobPath);
            if (cachedPath != null && File.Exists(cachedPath))
            {
                return CreateSequentialReadStream(cachedPath);
            }

            // Download to file and cache
            var downloadedPath = DownloadBlobToFile(blobPath);
            if (downloadedPath == null)
            {
                return null;
            }

            CacheFilePath(blobPath, downloadedPath);
            return CreateSequentialReadStream(downloadedPath);
        }
        finally
        {
            downloadLock.Release();
        }
    }

    /// <summary>
    /// Gets or creates a download lock for the specified blob path.
    /// Ensures only one download happens per blob at a time.
    /// </summary>
    private SemaphoreSlim GetOrCreateDownloadLock(string blobPath)
    {
        lock (_cacheLock)
        {
            if (_downloadLocks.TryGetValue(blobPath, out var downloadLock))
            {
                return downloadLock;
            }
            downloadLock = new SemaphoreSlim(1, 1);
            _downloadLocks[blobPath] = downloadLock;
            return downloadLock;
        }
    }

    /// <summary>
    /// Attempts to retrieve cached local file path for the specified blob and updates LRU order.
    /// </summary>
    /// <returns>The cached local file path if found; otherwise, null.</returns>
    private string? TryGetCachedFilePath(string blobPath)
    {
        lock (_cacheLock)
        {
            if (!_filePathCache.TryGetValue(blobPath, out var cachedPath))
            {
                return null;
            }

            // Update LRU: move to end (most recently used)
            if (_lruNodes.TryGetValue(blobPath, out var node))
            {
                _lruOrder.Remove(node);
                _lruOrder.AddLast(node);
            }

            return cachedPath;
        }
    }

    /// <summary>
    /// Checks if a blob is already cached without updating LRU order.
    /// Used for fast-path checks in prefetch scenarios.
    /// </summary>
    private bool IsCached(string blobPath)
    {
        lock (_cacheLock)
        {
            return _filePathCache.ContainsKey(blobPath);
        }
    }

    /// <summary>
    /// Downloads the specified blob to a local temp file and returns the file path.
    /// Returns null when the blob does not exist (404).
    /// </summary>
    private string? DownloadBlobToFile(string blobPath)
    {
        var blobClient = _containerClient.GetBlobClient(blobPath);

        var finalPath = Path.Combine(_tempDirectory, Uri.EscapeDataString(blobPath).Replace('/', Path.DirectorySeparatorChar));
        var tempPath = finalPath + ".tmp";

        try
        {
            Directory.CreateDirectory(Path.GetDirectoryName(finalPath)!);

            blobClient.DownloadTo(tempPath);

            File.Delete(finalPath);
            File.Move(tempPath, finalPath);

            return finalPath;
        }
        catch (RequestFailedException ex) when (ex.Status == 404)
        {
            try { if (File.Exists(tempPath)) File.Delete(tempPath); } catch { /* ignore */ }
            return null;
        }
    }

    /// <summary>
    /// Asynchronously downloads the specified blob to a local temp file and returns the file path.
    /// Returns null when the blob does not exist (404).
    /// </summary>
    private async Task<string?> DownloadBlobToFileAsync(string blobPath)
    {
        var blobClient = _containerClient.GetBlobClient(blobPath);

        var finalPath = Path.Combine(_tempDirectory, Uri.EscapeDataString(blobPath).Replace('/', Path.DirectorySeparatorChar));
        var tempPath = finalPath + ".tmp";

        try
        {
            Directory.CreateDirectory(Path.GetDirectoryName(finalPath)!);

            await blobClient.DownloadToAsync(tempPath).ConfigureAwait(false);

            File.Delete(finalPath);
            File.Move(tempPath, finalPath);

            return finalPath;
        }
        catch (RequestFailedException ex) when (ex.Status == 404)
        {
            try { if (File.Exists(tempPath)) File.Delete(tempPath); } catch { /* ignore */ }
            return null;
        }
    }

    /// <summary>
    /// Adds a local file path to the cache with LRU eviction if needed.
    /// </summary>
    private void CacheFilePath(string blobPath, string filePath)
    {
        var fileInfo = new FileInfo(filePath);
        var dataSize = fileInfo.Length;

        lock (_cacheLock)
        {
            // If already cached (race condition), don't cache again
            if (_filePathCache.ContainsKey(blobPath))
            {
                try { if (File.Exists(filePath)) File.Delete(filePath); } catch { /* ignore */ }
                return;
            }

            // Evict least recently used items until we have space
            while (_currentCacheSizeBytes + dataSize > _maxCacheSizeBytes && _lruOrder.Count > 0)
            {
                var oldestKey = _lruOrder.First!.Value;
                EvictCacheItem(oldestKey);
            }

            // Add new item to cache
            _filePathCache[blobPath] = filePath;
            var node = _lruOrder.AddLast(blobPath);
            _lruNodes[blobPath] = node;
            _currentCacheSizeBytes += dataSize;
        }
    }

    /// <summary>
    /// Evicts a single item from the cache and deletes its local file.
    /// </summary>
    private void EvictCacheItem(string blobPath)
    {
        if (_filePathCache.TryGetValue(blobPath, out var filePath))
        {
            try
            {
                var fi = new FileInfo(filePath);
                _currentCacheSizeBytes -= fi.Exists ? fi.Length : 0;
                if (fi.Exists) fi.Delete();
            }
            catch
            {
                // ignore file deletion errors
            }

            _filePathCache.Remove(blobPath);
        }

        if (_lruNodes.TryGetValue(blobPath, out var node))
        {
            _lruOrder.Remove(node);
            _lruNodes.Remove(blobPath);
        }
    }

    public IEnumerable<Column> GetColumns(string filePath)
    {
        using var stream = OpenAndValidateStream(filePath);
        foreach (var column in ParquetStreamReader.GetColumnsFromStream(stream))
        {
            yield return column;
        }
    }

    public IEnumerable<ParquetRow> ReadRows(string filePath, IEnumerable<string> columns)
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

        var options = new ParallelOptions { MaxDegreeOfParallelism = maxParallelism };
        await Parallel.ForEachAsync(filePaths, options, async (blobPath, ct) =>
        {
            await PrefetchBlobAsync(blobPath, ct);
        });
    }

    /// <summary>
    /// Downloads and caches a single blob without creating a MemoryStream wrapper.
    /// Optimized for prefetch scenarios where the stream object is not needed.
    /// </summary>
    private async Task PrefetchBlobAsync(string blobPath, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(blobPath);

        // Fast path: check if already cached
        if (IsCached(blobPath))
        {
            return;
        }

        var downloadLock = GetOrCreateDownloadLock(blobPath);

        // Acquire blob-specific lock to prevent concurrent downloads
        await downloadLock.WaitAsync(cancellationToken);
        try
        {
            // Double-check cache after acquiring lock
            if (IsCached(blobPath))
            {
                return;
            }

            // Download and cache (no stream creation)
            var downloadedPath = await DownloadBlobToFileAsync(blobPath);
            if (downloadedPath != null)
            {
                CacheFilePath(blobPath, downloadedPath);
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
    private FileStream OpenAndValidateStream(string filePath)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(filePath);

        var stream = OpenStream(filePath);
        return stream ?? throw new FileNotFoundException($"Blob not found: {filePath}", filePath);
    }

    /// <summary>
    /// Creates a read-only FileStream optimized for sequential reads.
    /// </summary>
    private static FileStream CreateSequentialReadStream(string path)
    {
        return new FileStream(
            path, FileMode.Open, 
            FileAccess.Read, FileShare.Read, 
            bufferSize: 4096, options: FileOptions.SequentialScan);
    }
}