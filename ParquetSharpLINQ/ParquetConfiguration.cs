namespace ParquetSharpLINQ;

/// <summary>
/// Configuration constants for ParquetSharpLINQ.
/// Centralizes default values and tunable parameters for consistent behavior across the library.
/// </summary>
public static class ParquetConfiguration
{
    /// <summary>
    /// Default maximum number of parallel prefetch operations for Azure Blob Storage.
    /// </summary>
    public const int DefaultPrefetchParallelism = 8;
    
    /// <summary>
    /// Default maximum cache size in bytes for Azure Blob Storage prefetching (4 GB).
    /// </summary>
    public const long DefaultMaxCacheSizeBytes = 4L * 1024 * 1024 * 1024;
}
