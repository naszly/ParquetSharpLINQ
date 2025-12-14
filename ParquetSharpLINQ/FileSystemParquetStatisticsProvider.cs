using ParquetSharpLINQ.Models;

namespace ParquetSharpLINQ;

/// <summary>
/// File system implementation of IParquetStatisticsProvider.
/// Reads Parquet file metadata and statistics from local files using ParquetSharp.
/// </summary>
public class FileSystemParquetStatisticsProvider : IParquetStatisticsProvider
{
    private readonly ParquetStatisticsExtractor _extractor;

    public FileSystemParquetStatisticsProvider()
        : this(new ParquetStatisticsExtractor())
    {
    }

    public FileSystemParquetStatisticsProvider(ParquetStatisticsExtractor extractor)
    {
        _extractor = extractor ?? throw new ArgumentNullException(nameof(extractor));
    }

    /// <inheritdoc />
    public async Task<ParquetFile> EnrichAsync(
        ParquetFile file,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(file);

        var fileInfo = new FileInfo(file.Path);
        
        // Use async file stream to avoid blocking thread pool threads
        await using var stream = new FileStream(
            file.Path,
            FileMode.Open,
            FileAccess.Read,
            FileShare.Read,
            bufferSize: 4096,
            useAsync: true);

        var enrichedFile = _extractor.ExtractFromStream(
            stream,
            file,
            fileInfo.Length,
            fileInfo.LastWriteTimeUtc);

        return enrichedFile;
    }
}

