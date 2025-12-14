using Azure.Storage.Blobs;
using ParquetSharpLINQ.Interfaces;
using ParquetSharpLINQ.Models;
using ParquetSharpLINQ.Statistics;

namespace ParquetSharpLINQ.Azure;

public sealed class AzureBlobParquetStatisticsProvider : IParquetStatisticsProvider
{
    private readonly BlobContainerClient _containerClient;
    private readonly ParquetStatisticsExtractor _extractor;

    public AzureBlobParquetStatisticsProvider(BlobContainerClient containerClient)
        : this(containerClient, new ParquetStatisticsExtractor())
    {
    }

    public AzureBlobParquetStatisticsProvider(
        BlobContainerClient containerClient,
        ParquetStatisticsExtractor extractor)
    {
        _containerClient = containerClient ?? throw new ArgumentNullException(nameof(containerClient));
        _extractor = extractor ?? throw new ArgumentNullException(nameof(extractor));
    }

    public async Task<ParquetFile> EnrichAsync(
        ParquetFile file,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(file);

        var blob = _containerClient.GetBlobClient(file.Path);
        var props = await blob.GetPropertiesAsync(cancellationToken: cancellationToken);
        var size = props.Value.ContentLength;

        await using var stream = new AzureBlobRangeStream(blob, size);

        var enrichedFile = _extractor.ExtractFromStream(
            stream,
            file,
            size,
            props.Value.LastModified);

        return enrichedFile;
    }
}
