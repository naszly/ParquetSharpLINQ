using Azure.Core;
using Azure.Core.Pipeline;
using Azure.Storage.Blobs;
using ParquetSharpLINQ.Interfaces;

namespace ParquetSharpLINQ.Azure;

/// <summary>
/// Extension methods for ParquetTableFactory to add Azure Blob Storage support.
/// Lives in the ParquetSharpLINQ.Azure assembly to keep Azure dependencies separate from the core library.
/// </summary>
public static class ParquetTableFactoryExtensions
{
    // Shared HTTP client configuration for all Azure operations
    // HttpClient is thread-safe and designed to be reused
    private static readonly Lazy<HttpClient> SharedHttpClient = new(CreateOptimizedHttpClient);
    private static readonly Lazy<BlobClientOptions> SharedBlobOptions = new(CreateOptimizedBlobOptions);

    /// <param name="factory">The ParquetTableFactory instance (use ParquetTable&lt;T&gt;.Factory)</param>
    extension<T>(ParquetTableFactory<T> factory) where T : new()
    {
        /// <summary>
        /// Creates a new ParquetTable for querying Parquet files from Azure Blob Storage.
        /// Supports both Hive-style partitioning and Delta Lake tables.
        /// Uses a shared, optimized HttpClient across all instances for connection pooling and HTTP/2 support.
        /// </summary>
        /// <param name="connectionString">Azure Storage connection string</param>
        /// <param name="containerName">Blob container name</param>
        /// <param name="blobPrefix">Optional blob prefix/subfolder path (e.g., "data/sales/" or empty for root)</param>
        /// <param name="cacheExpiration">Optional cache expiration for Delta log (default: 5 minutes)</param>
        /// <param name="maxCacheSizeBytes">Optional max cache size for blob downloads</param>
        /// <param name="mapper">Optional custom mapper (for DI/testing). If null, uses source-generated mapper.</param>
        /// <returns>A new ParquetTable instance for querying Azure Blob Storage</returns>
        /// <example>
        /// var table = ParquetTable&lt;SalesRecord&gt;.Factory.FromAzureBlob(connectionString, "sales-data");
        /// </example>
        public ParquetTable<T> FromAzureBlob(string connectionString,
            string containerName,
            string blobPrefix = "",
            TimeSpan? cacheExpiration = null,
            long maxCacheSizeBytes = ParquetConfiguration.DefaultMaxCacheSizeBytes,
            IParquetMapper<T>? mapper = null)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(connectionString);
            ArgumentException.ThrowIfNullOrWhiteSpace(containerName);

            var containerClient = CreateOptimizedContainerClient(connectionString, containerName);
        
            var discoveryStrategy = new AzureBlobPartitionDiscovery(containerClient, blobPrefix, cacheExpiration);
            var reader = new AzureBlobParquetReader(containerClient, maxCacheSizeBytes);
            return new ParquetTable<T>(discoveryStrategy, reader, mapper);
        }

        /// <summary>
        /// Creates a new ParquetTable for querying Parquet files from Azure Blob Storage using an existing container client.
        /// Supports both Hive-style partitioning and Delta Lake tables.
        /// </summary>
        /// <param name="containerClient">Existing blob container client</param>
        /// <param name="blobPrefix">Optional blob prefix/subfolder path (e.g., "data/sales/" or empty for root)</param>
        /// <param name="cacheExpiration">Optional cache expiration for Delta log (default: 5 minutes)</param>
        /// <param name="maxCacheSizeBytes">Optional max cache size for blob downloads</param>
        /// <param name="mapper">Optional custom mapper (for DI/testing). If null, uses source-generated mapper.</param>
        /// <param name="reader">Optional custom reader (for DI/testing). If null, uses AzureBlobParquetReader.</param>
        /// <returns>A new ParquetTable instance for querying Azure Blob Storage</returns>
        /// <example>
        /// var table = ParquetTable&lt;SalesRecord&gt;.Factory.FromAzureBlob(containerClient);
        /// </example>
        public ParquetTable<T> FromAzureBlob(BlobContainerClient containerClient,
            string blobPrefix = "",
            TimeSpan? cacheExpiration = null,
            long maxCacheSizeBytes = ParquetConfiguration.DefaultMaxCacheSizeBytes,
            IParquetMapper<T>? mapper = null,
            IAsyncParquetReader? reader = null)
        {
            ArgumentNullException.ThrowIfNull(containerClient);
        
            var statisticsProvider = new AzureBlobParquetStatisticsProvider(containerClient);
            
            var discoveryStrategy = new AzureBlobPartitionDiscovery(
                containerClient, 
                blobPrefix, 
                cacheExpiration,
                statisticsProvider);
            reader ??= new AzureBlobParquetReader(containerClient, maxCacheSizeBytes);
            return new ParquetTable<T>(discoveryStrategy, reader, mapper);
        }
    }

    /// <summary>
    /// Creates a new BlobContainerClient configured with shared, optimized HTTP/2 settings.
    /// The underlying HttpClient and BlobClientOptions are shared across all instances for connection pooling.
    /// </summary>
    private static BlobContainerClient CreateOptimizedContainerClient(string connectionString, string containerName)
    {
        var serviceClient = new BlobServiceClient(connectionString, SharedBlobOptions.Value);
        return serviceClient.GetBlobContainerClient(containerName);
    }

    /// <summary>
    /// Creates a shared, optimized HttpClient configured for Azure Blob Storage.
    /// This is thread-safe and intended to be reused across all operations.
    /// </summary>
    private static HttpClient CreateOptimizedHttpClient()
    {
        var socketsHttpHandler = new SocketsHttpHandler
        {
            PooledConnectionLifetime = TimeSpan.FromMinutes(10),
            PooledConnectionIdleTimeout = TimeSpan.FromMinutes(5),
            MaxConnectionsPerServer = 100,
            EnableMultipleHttp2Connections = true,
            ConnectTimeout = TimeSpan.FromSeconds(30),
            ResponseDrainTimeout = TimeSpan.FromSeconds(2)
        };

        return new HttpClient(socketsHttpHandler)
        {
            Timeout = TimeSpan.FromMinutes(10),
            DefaultRequestVersion = new Version(2, 0)
        };
    }

    /// <summary>
    /// Creates shared BlobClientOptions configured for high-throughput operations.
    /// </summary>
    private static BlobClientOptions CreateOptimizedBlobOptions()
    {
        return new BlobClientOptions
        {
            Transport = new HttpClientTransport(SharedHttpClient.Value),
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
    }
}


