using Azure.Storage.Blobs;
using ParquetSharpLINQ.Attributes;
using ParquetSharpLINQ.Azure;

namespace ParquetSharpLINQ.Tests.Integration;

[TestFixture]
[Category("Integration")]
public class AzureDeltaLakeIntegrationTests : DeltaLakeIntegrationTestsBase
{
    private const string AzuriteConnectionString = "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;";
    
    private static readonly string LocalDeltaPath = Path.Combine(
        TestContext.CurrentContext.TestDirectory,
        "..", "..", "..", "..",
        "ParquetSharpLINQ.Tests", "Integration", "delta_test_data");

    private BlobServiceClient? _blobServiceClient;
    private readonly List<string> _containersToCleanup = [];
    
    private static readonly Dictionary<string, string> TableNameToContainerMapping = new()
    {
        { "simple_delta", "simple-delta" },
        { "partitioned_delta", "partitioned-delta" },
        { "delta_with_updates", "delta-with-updates" },
        { "delta_string_partitions", "delta-string-partitions" }
    };

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        if (!Directory.Exists(LocalDeltaPath))
        {
            Assert.Inconclusive(
                $"Delta test data not found at {LocalDeltaPath}. " +
                "Run 'python3 Integration/generate_delta_test_data.py' to generate test data first.");
        }

        try
        {
            _blobServiceClient = new BlobServiceClient(AzuriteConnectionString);
            
            await UploadDeltaTableToAzure("simple-delta", "simple_delta");
            await UploadDeltaTableToAzure("partitioned-delta", "partitioned_delta");
            await UploadDeltaTableToAzure("delta-with-updates", "delta_with_updates");
            await UploadDeltaTableToAzure("delta-string-partitions", "delta_string_partitions");
        }
        catch (Exception ex)
        {
            Assert.Inconclusive(
                $"Could not connect to Azurite. Make sure Azurite is running on port 10000. " +
                $"Error: {ex.Message}");
        }
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
        if (_blobServiceClient != null)
        {
            foreach (var containerName in _containersToCleanup)
            {
                try
                {
                    var container = _blobServiceClient.GetBlobContainerClient(containerName);
                    await container.DeleteIfExistsAsync();
                }
                catch
                {
                    // Ignore cleanup errors
                }
            }
        }
    }

    protected override HiveParquetTable<T> CreateTable<T>(string tableName)
    {
        if (_blobServiceClient == null)
        {
            Assert.Inconclusive("Azurite not available");
        }

        if (!TableNameToContainerMapping.TryGetValue(tableName, out var containerName))
        {
            Assert.Fail($"Unknown table name: {tableName}");
        }

        return new AzureHiveParquetTable<T>(AzuriteConnectionString, containerName);
    }

    private async Task UploadDeltaTableToAzure(string containerName, string tableName)
    {
        var localTablePath = Path.Combine(LocalDeltaPath, tableName);
        if (!Directory.Exists(localTablePath))
        {
            return;
        }

        var containerClient = _blobServiceClient!.GetBlobContainerClient(containerName);
        await containerClient.CreateIfNotExistsAsync();
        _containersToCleanup.Add(containerName);

        var files = Directory.GetFiles(localTablePath, "*.*", SearchOption.AllDirectories);
        
        foreach (var file in files)
        {
            var relativePath = Path.GetRelativePath(localTablePath, file);
            var blobName = relativePath.Replace(Path.DirectorySeparatorChar, '/');
            
            var blobClient = containerClient.GetBlobClient(blobName);
            await using var fileStream = File.OpenRead(file);
            await blobClient.UploadAsync(fileStream, overwrite: true);
        }
    }

    [Test]
    public void DeltaLog_ExistsAndContainsJsonFiles()
    {
        if (_blobServiceClient == null)
        {
            Assert.Inconclusive("Azurite not available");
        }

        var containerClient = _blobServiceClient.GetBlobContainerClient("simple-delta");
        var deltaLogPrefix = "_delta_log/";
        var deltaLogBlobs = containerClient.GetBlobs(prefix: deltaLogPrefix);

        var jsonFiles = deltaLogBlobs
            .Where(b => b.Name.EndsWith(".json", StringComparison.OrdinalIgnoreCase))
            .ToList();

        Assert.That(jsonFiles, Is.Not.Empty,
            "Delta log should contain JSON transaction log files");
    }
}
