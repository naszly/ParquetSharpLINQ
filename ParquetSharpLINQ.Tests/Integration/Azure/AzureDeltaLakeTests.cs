using System.Diagnostics;
using Azure.Storage.Blobs;
using ParquetSharpLINQ.Azure;
using ParquetSharpLINQ.Tests.Integration.Delta;
using ParquetSharpLINQ.Tests.Integration.Helpers;

namespace ParquetSharpLINQ.Tests.Integration.Azure;

[TestFixture]
[Category("Integration")]
public class AzureDeltaLakeTests : DeltaLakeTestsBase
{
    private static readonly string LocalDeltaPath = Path.Combine(
        TestContext.CurrentContext.TestDirectory,
        "..", "..", "..", "..",
        "ParquetSharpLINQ.Tests", "Integration", "Delta", "delta_test_data");

    private BlobServiceClient? _blobServiceClient;
    private readonly List<string> _containersToCleanup = [];

    private static readonly Dictionary<string, string> TableNameToContainerMapping = new()
    {
        { "simple_delta", "simple-delta" },
        { "partitioned_delta", "partitioned-delta" },
        { "delta_with_updates", "delta-with-updates" },
        { "delta_string_partitions", "delta-string-partitions" }
    };
    
    private BlobServiceClient BlobServiceClient => 
        _blobServiceClient ?? throw new InvalidOperationException("BlobServiceClient is not initialized");

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        if (!Directory.Exists(LocalDeltaPath))
        {
            Assert.Inconclusive(
                $"Delta test data not found at {LocalDeltaPath}. " +
                "Run 'python3 Integration/Delta/generate_delta_test_data.py' to generate test data first.");
        }

        try
        {
            _blobServiceClient = new BlobServiceClient(AzuriteTestHelper.ConnectionString);

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

    protected override ParquetTable<T> CreateTable<T>(string tableName)
    {
        if (!TableNameToContainerMapping.TryGetValue(tableName, out var containerName))
        {
            Assert.Fail($"Unknown table name: {tableName}");
        }

        Debug.Assert(containerName != null, nameof(containerName) + " != null");
        return ParquetTable<T>.Factory.FromAzureBlob(AzuriteTestHelper.ConnectionString, containerName);
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
        var containerClient = BlobServiceClient.GetBlobContainerClient("simple-delta");
        var deltaLogPrefix = "_delta_log/";
        var deltaLogBlobs = containerClient.GetBlobs(prefix: deltaLogPrefix);

        var jsonFiles = deltaLogBlobs
            .Where(b => b.Name.EndsWith(".json", StringComparison.OrdinalIgnoreCase))
            .ToList();

        Assert.That(jsonFiles, Is.Not.Empty,
            "Delta log should contain JSON transaction log files");
    }

    [Test]
    public async Task DeltaTable_WithBlobPrefix_CanBeQueried()
    {
        var containerName = $"delta-prefix-test-{Guid.NewGuid():N}";
        var blobPrefix = "analytics/tables/";
        var containerClient = BlobServiceClient.GetBlobContainerClient(containerName);

        await containerClient.CreateIfNotExistsAsync();
        _containersToCleanup.Add(containerName);

        await UploadDeltaTableWithPrefix(containerClient, "simple_delta", blobPrefix);

        using var table = ParquetTable<SimpleDeltaRecord>.Factory.FromAzureBlob(
            AzuriteTestHelper.ConnectionString,
            containerName,
            blobPrefix);

        var results = table.ToList();

        Assert.That(results, Has.Count.EqualTo(5));
        Assert.That(results.Select(r => r.Name), Contains.Item("Alice"));
        Assert.That(results.Select(r => r.Name), Contains.Item("Bob"));
    }

    [Test]
    public async Task PartitionedDeltaTable_WithBlobPrefix_PartitionPruningWorks()
    {
        var containerName = $"delta-partition-prefix-{Guid.NewGuid():N}";
        var blobPrefix = "warehouse/partitioned/";
        var containerClient = BlobServiceClient.GetBlobContainerClient(containerName);

        await containerClient.CreateIfNotExistsAsync();
        _containersToCleanup.Add(containerName);

        await UploadDeltaTableWithPrefix(containerClient, "partitioned_delta", blobPrefix);

        using var table = ParquetTable<PartitionedDeltaRecord>.Factory.FromAzureBlob(
            AzuriteTestHelper.ConnectionString,
            containerName,
            blobPrefix);

        var results = table
            .Where(r => r.Year == 2024 && r.Month == 6)
            .ToList();

        Assert.That(results, Is.Not.Empty);
        Assert.That(results.All(r => r.Year == 2024), Is.True);
        Assert.That(results.All(r => r.Month == 6), Is.True);
        Assert.That(results, Has.Count.EqualTo(5));

        // Verify delta log exists at the correct path with prefix
        var deltaLogPrefix = blobPrefix + "_delta_log/";
        var deltaLogBlobs = containerClient.GetBlobs(prefix: deltaLogPrefix);
        Assert.That(deltaLogBlobs.Any(), Is.True, "Delta log should exist in the subfolder");
    }

    [Test]
    public async Task DeltaTable_WithBlobPrefix_DoesNotReadRootData()
    {
        var containerName = $"delta-isolation-{Guid.NewGuid():N}";
        var blobPrefix = "subfolder/data/";
        var containerClient = BlobServiceClient.GetBlobContainerClient(containerName);

        await containerClient.CreateIfNotExistsAsync();
        _containersToCleanup.Add(containerName);

        // Upload one table to root
        await UploadDeltaTableWithPrefix(containerClient, "simple_delta", "");

        // Upload another table to subfolder with different data
        await UploadDeltaTableWithPrefix(containerClient, "delta_with_updates", blobPrefix);

        // Read from subfolder should only get delta_with_updates (6 records)
        using var tableWithPrefix = ParquetTable<DeltaProductRecord>.Factory.FromAzureBlob(
            AzuriteTestHelper.ConnectionString,
            containerName,
            blobPrefix);

        var prefixResults = tableWithPrefix.ToList();
        Assert.That(prefixResults, Has.Count.EqualTo(6),
            "Should only read from subfolder, not root");

        // Verify root still has simple_delta (5 records)
        using var rootTable = ParquetTable<SimpleDeltaRecord>.Factory.FromAzureBlob(
            AzuriteTestHelper.ConnectionString,
            containerName,
            "");

        var rootResults = rootTable.ToList();
        Assert.That(rootResults, Has.Count.EqualTo(5),
            "Root should have its own data");
    }

    private async Task UploadDeltaTableWithPrefix(BlobContainerClient containerClient, string tableName,
        string blobPrefix)
    {
        var localTablePath = Path.Combine(LocalDeltaPath, tableName);
        if (!Directory.Exists(localTablePath))
        {
            Assert.Inconclusive($"{tableName} test data not found");
        }

        var files = Directory.GetFiles(localTablePath, "*.*", SearchOption.AllDirectories);

        foreach (var file in files)
        {
            var relativePath = Path.GetRelativePath(localTablePath, file);
            var blobName = blobPrefix + relativePath.Replace(Path.DirectorySeparatorChar, '/');

            var blobClient = containerClient.GetBlobClient(blobName);
            await using var fileStream = File.OpenRead(file);
            await blobClient.UploadAsync(fileStream, overwrite: true);
        }
    }
}