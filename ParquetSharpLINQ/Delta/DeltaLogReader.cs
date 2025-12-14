namespace ParquetSharpLINQ.Delta;

#if !NET9_0_OR_GREATER
using Lock = System.Object;
#endif

public class DeltaLogReader
{
    private const int DefaultCacheExpirationMinutes = 5;
    
    private readonly string _deltaLogPath;
    private readonly TimeSpan _cacheExpiration;
    
    private DeltaSnapshot? _cachedSnapshot;
    private DateTime _snapshotCacheExpiry = DateTime.MinValue;
    private readonly Lock _cacheLock = new();

    public DeltaLogReader(string tablePath, TimeSpan? cacheExpiration = null)
    {
        _deltaLogPath = Path.Combine(tablePath, "_delta_log");
        _cacheExpiration = cacheExpiration ?? TimeSpan.FromMinutes(DefaultCacheExpirationMinutes);
    }

    /// <summary>
    /// Clears all cached data, forcing a fresh read from the file system on next access.
    /// </summary>
    public void ClearCache()
    {
        lock (_cacheLock)
        {
            _cachedSnapshot = null;
            _snapshotCacheExpiry = DateTime.MinValue;
        }
    }

    public DeltaSnapshot GetLatestSnapshot()
    {
        lock (_cacheLock)
        {
            if (_cachedSnapshot != null && DateTime.UtcNow < _snapshotCacheExpiry)
            {
                return _cachedSnapshot;
            }
            
            if (!Directory.Exists(_deltaLogPath))
            {
                throw new InvalidOperationException($"Not a Delta table. Missing _delta_log directory at {_deltaLogPath}");
            }

            var allActions = GetLogFiles().SelectMany(ReadLogFile);
            _cachedSnapshot = DeltaLogParser.BuildSnapshot(allActions);
            _snapshotCacheExpiry = DateTime.UtcNow.Add(_cacheExpiration);
            
            return _cachedSnapshot;
        }
    }
    
    private IEnumerable<string> GetLogFiles()
    {
        var jsonFiles = Directory
            .GetFiles(_deltaLogPath, "*.json")
            .OrderBy(DeltaLogParser.ExtractVersion);

        foreach (var file in jsonFiles)
        {
            yield return file;
        }
    }

    private static IEnumerable<DeltaAction> ReadLogFile(string filePath)
    {
        foreach (var line in File.ReadLines(filePath))
        {
            if (string.IsNullOrWhiteSpace(line))
            {
                continue;
            }

            var action = DeltaLogParser.ParseAction(line);
            if (action != null)
            {
                yield return action;
            }
        }
    }
}


