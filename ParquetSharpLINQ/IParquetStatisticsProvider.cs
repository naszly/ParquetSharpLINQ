using ParquetSharpLINQ.Models;

namespace ParquetSharpLINQ;

/// <summary>
/// Provides the ability to enrich Parquet file metadata with statistics.
/// Implementers can read row-group and column-level statistics for query optimization.
/// </summary>
public interface IParquetStatisticsProvider
{
    /// <summary>
    /// Enriches a ParquetFile with row-group and column-level statistics.
    /// </summary>
    /// <param name="file">The ParquetFile to enrich with statistics</param>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>A new ParquetFile instance with populated statistics</returns>
    Task<ParquetFile> EnrichAsync(
        ParquetFile file,
        CancellationToken cancellationToken = default);
}

