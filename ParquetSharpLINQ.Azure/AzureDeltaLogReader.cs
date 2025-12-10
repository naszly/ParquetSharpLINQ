using System.Collections.Immutable;
using Azure.Storage.Blobs;
using ParquetSharpLINQ.Delta;

namespace ParquetSharpLINQ.Azure;

#if !NET9_0_OR_GREATER
using Lock = System.Object;
#endif

/// <summary>
/// Reads Delta Lake transaction logs from Azure Blob Storage.
/// </summary>
public class AzureDeltaLogReader
{
    private const int DefaultCacheExpirationMinutes = 5;
    
    private readonly BlobContainerClient _containerClient;
    private readonly string _deltaLogPrefix;
    private readonly TimeSpan _cacheExpiration;
    
    private ImmutableArray<(string BlobName, long Version)>? _cachedLogFiles;
    private DateTime _logFilesCacheExpiry = DateTime.MinValue;
    private readonly Lock _logFilesLock = new();
    
    private readonly Dictionary<string, ImmutableArray<DeltaAction>> _logFileContentsCache = new(StringComparer.OrdinalIgnoreCase);
    private readonly Lock _contentsLock = new();

    public AzureDeltaLogReader(BlobContainerClient containerClient, string blobPrefix = "", TimeSpan? cacheExpiration = null)
    {
        _containerClient = containerClient ?? throw new ArgumentNullException(nameof(containerClient));
        _cacheExpiration = cacheExpiration ?? TimeSpan.FromMinutes(DefaultCacheExpirationMinutes);
        
        blobPrefix = blobPrefix?.Trim() ?? "";
        if (!string.IsNullOrEmpty(blobPrefix))
        {
            if (blobPrefix.StartsWith('/'))
            {
                blobPrefix = blobPrefix.TrimStart('/');
            }
            if (!blobPrefix.EndsWith('/'))
            {
                blobPrefix += '/';
            }
        }
        
        _deltaLogPrefix = blobPrefix + "_delta_log/";
    }
    
    /// <summary>
    /// Clears all cached data, forcing a fresh read from Azure Blob Storage on next access.
    /// </summary>
    public void ClearCache()
    {
        lock (_logFilesLock)
        {
            _cachedLogFiles = null;
            _logFilesCacheExpiry = DateTime.MinValue;
        }
        
        lock (_contentsLock)
        {
            _logFileContentsCache.Clear();
        }
    }

    /// <summary>
    /// Gets the latest Delta Lake snapshot by reading and parsing all transaction log files.
    /// Uses cached data when available to minimize Azure Storage API calls.
    /// </summary>
    /// <returns>A Delta snapshot containing active files, metadata, and protocol information.</returns>
    public DeltaSnapshot GetLatestSnapshot()
    {
        var allActions = GetLogFilesWithCache()
            .SelectMany(f => ReadLogFileWithCache(f.BlobName));
        
        return DeltaLogParser.BuildSnapshot(allActions);
    }

    private ImmutableArray<(string BlobName, long Version)> GetLogFilesWithCache()
    {
        lock (_logFilesLock)
        {
            if (_cachedLogFiles != null && DateTime.UtcNow < _logFilesCacheExpiry)
            {
                return _cachedLogFiles.Value;
            }
            
            _cachedLogFiles = GetLogFiles()
                .OrderBy(f => f.Version)
                .ToImmutableArray();
            
            _logFilesCacheExpiry = DateTime.UtcNow.Add(_cacheExpiration);
            
            return _cachedLogFiles.Value;
        }
    }

    private IEnumerable<(string BlobName, long Version)> GetLogFiles()
    {
        var blobs = _containerClient.GetBlobs(prefix: _deltaLogPrefix);

        foreach (var blob in blobs)
        {
            if (!blob.Name.EndsWith(".json", StringComparison.OrdinalIgnoreCase))
            {
                continue;
            }
            
            var version = DeltaLogParser.ExtractVersion(blob.Name);
            if (version >= 0)
            {
                yield return (blob.Name, version);
            }
        }
    }

    private ImmutableArray<DeltaAction> ReadLogFileWithCache(string blobName)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(blobName);

        lock (_contentsLock)
        {
            if (_logFileContentsCache.TryGetValue(blobName, out var cachedContent))
            {
                return cachedContent;
            }
            var content = ReadLogFile(blobName).ToImmutableArray();
            _logFileContentsCache[blobName] = content;
            
            return content;
        }
    }

    private IEnumerable<DeltaAction> ReadLogFile(string blobName)
    {
        var blobClient = _containerClient.GetBlobClient(blobName);
        
        if (!blobClient.Exists())
        {
            yield break;
        }

        using var stream = new MemoryStream();
        blobClient.DownloadTo(stream);
        stream.Position = 0;

        using var reader = new StreamReader(stream);
        
        foreach (var action in DeltaLogParser.ParseActionsFromStream(reader))
        {
            yield return action;
        }
    }
}

