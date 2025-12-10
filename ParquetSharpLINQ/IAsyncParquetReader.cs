namespace ParquetSharpLINQ;

/// <summary>
/// Extended interface for Parquet readers that support async operations and prefetching.
/// Implemented by readers that can benefit from parallel blob downloads (e.g., Azure Blob Storage).
/// </summary>
public interface IAsyncParquetReader : IParquetReader
{
    /// <summary>
    /// Prefetches multiple files into cache in parallel.
    /// </summary>
    /// <param name="filePaths">Paths to prefetch</param>
    /// <param name="maxParallelism">Maximum number of concurrent downloads</param>
    Task PrefetchAsync(IEnumerable<string> filePaths, int maxParallelism = ParquetConfiguration.DefaultPrefetchParallelism);
}
