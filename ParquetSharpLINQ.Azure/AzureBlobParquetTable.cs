using Azure.Storage.Blobs;
using ParquetSharpLINQ.Models;

namespace ParquetSharpLINQ.Azure;

/// <summary>
/// Parquet table backed by Azure Blob Storage with partition discovery support.
/// Supports both Hive-style partitioning and Delta Lake tables.
/// </summary>
public sealed class AzureBlobParquetTable<T> : ParquetTable<T> where T : new()
{
    private readonly BlobContainerClient _containerClient;
    private readonly string _blobPrefix;
    private readonly Lazy<AzureDeltaLogReader> _deltaLogReader;

    /// <summary>
    /// Creates a new Azure Blob Storage-backed Parquet table.
    /// </summary>
    /// <param name="connectionString">Azure Storage connection string</param>
    /// <param name="containerName">Blob container name</param>
    /// <param name="blobPrefix">Optional blob prefix/subfolder path (e.g., "data/sales/" or empty for root)</param>
    /// <param name="mapper">Optional custom mapper</param>
    public AzureBlobParquetTable(
        string connectionString,
        string containerName,
        string blobPrefix = "",
        IParquetMapper<T>? mapper = null)
        : base(
            blobPrefix,
            new AzureBlobParquetReader(connectionString, containerName),
            mapper)
    {
        var serviceClient = new BlobServiceClient(connectionString);
        _containerClient = serviceClient.GetBlobContainerClient(containerName);
        _blobPrefix = blobPrefix;
        _deltaLogReader = new Lazy<AzureDeltaLogReader>(() => new AzureDeltaLogReader(_containerClient, blobPrefix));
    }

    /// <summary>
    /// Creates a new Azure Blob Storage-backed Parquet table with existing container client.
    /// </summary>
    /// <param name="containerClient">Existing blob container client</param>
    /// <param name="blobPrefix">Optional blob prefix/subfolder path (e.g., "data/sales/" or empty for root)</param>
    /// <param name="mapper">Optional custom mapper</param>
    public AzureBlobParquetTable(
        BlobContainerClient containerClient,
        string blobPrefix = "",
        IParquetMapper<T>? mapper = null)
        : base(
            blobPrefix,
            new AzureBlobParquetReader(containerClient),
            mapper)
    {
        _containerClient = containerClient ?? throw new ArgumentNullException(nameof(containerClient));
        _blobPrefix = blobPrefix;
        _deltaLogReader = new Lazy<AzureDeltaLogReader>(() => new AzureDeltaLogReader(_containerClient, blobPrefix));
    }

    /// <summary>
    /// Discovers partitions from Azure Blob Storage.
    /// Overrides base implementation to use Azure-specific discovery.
    /// Creates and caches a Delta log reader if needed for Delta tables.
    /// </summary>
    public override IEnumerable<Partition> DiscoverPartitions()
    {
        return AzurePartitionDiscovery.Discover(_containerClient, _deltaLogReader, _blobPrefix);
    }

    /// <summary>
    /// Clears the Delta log cache, forcing a fresh read from Azure Blob Storage on next access.
    /// Only has an effect if the table is a Delta table and the Delta log reader has been initialized.
    /// </summary>
    public void ClearDeltaLogCache()
    {
        if (_deltaLogReader.IsValueCreated)
        {
            _deltaLogReader.Value.ClearCache();
        }
    }
}

