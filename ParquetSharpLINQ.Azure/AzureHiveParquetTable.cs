using Azure.Storage.Blobs;
using ParquetSharpLINQ.Models;

namespace ParquetSharpLINQ.Azure;

/// <summary>
/// Specialized HiveParquetTable for Azure Blob Storage with partition discovery support.
/// </summary>
public sealed class AzureHiveParquetTable<T> : HiveParquetTable<T> where T : new()
{
    private readonly BlobContainerClient _containerClient;
    private readonly string _blobPrefix;

    /// <summary>
    /// Creates a new Azure Blob Storage-backed HiveParquetTable.
    /// </summary>
    /// <param name="connectionString">Azure Storage connection string</param>
    /// <param name="containerName">Blob container name</param>
    /// <param name="blobPrefix">Optional blob prefix/subfolder path (e.g., "data/sales/" or empty for root)</param>
    /// <param name="mapper">Optional custom mapper</param>
    public AzureHiveParquetTable(
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
    }

    /// <summary>
    /// Creates a new Azure Blob Storage-backed HiveParquetTable with existing container client.
    /// </summary>
    /// <param name="containerClient">Existing blob container client</param>
    /// <param name="blobPrefix">Optional blob prefix/subfolder path (e.g., "data/sales/" or empty for root)</param>
    /// <param name="mapper">Optional custom mapper</param>
    public AzureHiveParquetTable(
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
    }

    /// <summary>
    /// Discovers partitions from Azure Blob Storage.
    /// Overrides base implementation to use Azure-specific discovery.
    /// </summary>
    public override IEnumerable<Partition> DiscoverPartitions()
    {
        return AzurePartitionDiscovery.Discover(_containerClient, _blobPrefix);
    }
}

