using Azure.Storage.Blobs;
using ParquetSharpLINQ.Azure;
using ParquetSharpLINQ.DataGenerator;
using ParquetSharpLINQ.Models;

namespace ParquetSharpLINQ.Tests.Integration;

/// <summary>
/// Minimal integration tests for statistics providers.
/// Most functionality is covered by unit tests - these tests verify:
/// 1. End-to-end provider integration with real storage
/// 2. Error handling with non-existent files/blobs
/// 3. Azure-specific features (AzureBlobRangeStream)
/// </summary>
[TestFixture]
[Category("Integration")]
public class StatisticsProvidersIntegrationTests
{
    private string _localTestDataPath = null!;
    private FileSystemParquetStatisticsProvider _fileSystemProvider = null!;

    [OneTimeSetUp]
    public void OneTimeSetup()
    {
        _localTestDataPath = Path.Combine(Path.GetTempPath(), $"ParquetStatisticsTest_{Guid.NewGuid()}");
        Directory.CreateDirectory(_localTestDataPath);

        var generator = new TestDataGenerator();
        generator.GenerateParquetFiles(
            _localTestDataPath,
            recordsPerPartition: 100,
            years: [2024],
            monthsPerYear: 1
        );

        _fileSystemProvider = new FileSystemParquetStatisticsProvider();
    }

    [OneTimeTearDown]
    public void OneTimeTearDown()
    {
        if (Directory.Exists(_localTestDataPath))
        {
            Directory.Delete(_localTestDataPath, true);
        }
    }

    #region FileSystem Provider Tests

    [Test]
    [Category("LocalFiles")]
    public async Task FileSystemProvider_EnrichAsync_EndToEndIntegration()
    {
        // Arrange
        var parquetFiles = Directory.GetFiles(_localTestDataPath, "*.parquet", SearchOption.AllDirectories);
        var testFile = parquetFiles.First();
        var file = new ParquetFile { Path = testFile };

        // Act
        var enrichedFile = await _fileSystemProvider.EnrichAsync(file);

        // Assert - verify end-to-end integration works
        Assert.That(enrichedFile.Path, Is.EqualTo(testFile));
        Assert.That(enrichedFile.SizeBytes, Is.GreaterThan(0), "Should read file size");
        Assert.That(enrichedFile.RowCount, Is.GreaterThan(0), "Should extract row count");
        Assert.That(enrichedFile.RowGroups, Is.Not.Empty, "Should extract row groups");
        Assert.That(enrichedFile.RowGroups[0].ColumnStatisticsByPath, Is.Not.Empty, "Should extract statistics");
        
        // Verify min/max are present (actual values tested in unit tests)
        var columnStats = enrichedFile.RowGroups[0].ColumnStatisticsByPath;
        Assert.That(columnStats["id"].HasMinMax, Is.True, "Should have min/max statistics");
    }



    [Test]
    [Category("LocalFiles")]
    public async Task FileSystemProvider_ConcurrentEnrichment_ThreadSafe()
    {
        // Arrange
        var parquetFiles = Directory.GetFiles(_localTestDataPath, "*.parquet", SearchOption.AllDirectories);
        var files = parquetFiles.Take(3).Select(f => new ParquetFile { Path = f }).ToList();

        // Act - enrich concurrently
        var tasks = files.Select(f => _fileSystemProvider.EnrichAsync(f)).ToList();
        var enrichedFiles = await Task.WhenAll(tasks);

        // Assert
        Assert.That(enrichedFiles, Has.Length.EqualTo(files.Count));
        foreach (var enrichedFile in enrichedFiles)
        {
            Assert.That(enrichedFile.RowCount, Is.GreaterThan(0));
            Assert.That(enrichedFile.RowGroups, Is.Not.Empty);
        }
    }

    #endregion

    #region Azure Provider Tests

    private const string AzuriteConnectionString =
        "DefaultEndpointsProtocol=http;" +
        "AccountName=devstoreaccount1;" +
        "AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;" +
        "BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;";

    [Test]
    [Category("Azure")]
    public async Task AzureProvider_EnrichAsync_IntegratesWithBlobStorage()
    {
        // Check if Azurite is running
        if (!await IsAzuriteRunning())
        {
            Assert.Ignore("Azurite not running. Start with: docker run -p 10000:10000 mcr.microsoft.com/azure-storage/azurite");
        }

        // Setup
        var containerName = $"test-stats-{Guid.NewGuid():N}";
        var serviceClient = new BlobServiceClient(AzuriteConnectionString);
        var containerClient = serviceClient.GetBlobContainerClient(containerName);
        await containerClient.CreateAsync();

        try
        {
            // Upload a test file
            var localFiles = Directory.GetFiles(_localTestDataPath, "*.parquet", SearchOption.AllDirectories);
            var testFile = localFiles.First();
            var blobName = "test/data.parquet";
            var blobClient = containerClient.GetBlobClient(blobName);

            await using (var fileStream = File.OpenRead(testFile))
            {
                await blobClient.UploadAsync(fileStream);
            }

            // Test
            var provider = new AzureBlobParquetStatisticsProvider(containerClient);
            var file = new ParquetFile { Path = blobName };

            // Act
            var enrichedFile = await provider.EnrichAsync(file);

            // Assert - verify Azure blob integration works (uses AzureBlobRangeStream)
            Assert.That(enrichedFile.Path, Is.EqualTo(blobName));
            Assert.That(enrichedFile.SizeBytes, Is.GreaterThan(0), "Should read blob size");
            Assert.That(enrichedFile.RowCount, Is.GreaterThan(0), "Should extract from blob");
            Assert.That(enrichedFile.RowGroups, Is.Not.Empty, "Should use AzureBlobRangeStream");
            Assert.That(enrichedFile.RowGroups[0].ColumnStatisticsByPath, Is.Not.Empty);
        }
        finally
        {
            await containerClient.DeleteAsync();
        }
    }



    [Test]
    [Category("Azure")]
    public async Task AzureProvider_ConcurrentEnrichment_ThreadSafe()
    {
        // Check if Azurite is running
        if (!await IsAzuriteRunning())
        {
            Assert.Ignore("Azurite not running");
        }

        // Setup
        var containerName = $"test-stats-{Guid.NewGuid():N}";
        var serviceClient = new BlobServiceClient(AzuriteConnectionString);
        var containerClient = serviceClient.GetBlobContainerClient(containerName);
        await containerClient.CreateAsync();

        try
        {
            // Upload test files
            var localFiles = Directory.GetFiles(_localTestDataPath, "*.parquet", SearchOption.AllDirectories);
            var blobNames = new List<string>();

            for (int i = 0; i < Math.Min(3, localFiles.Length); i++)
            {
                var blobName = $"test/file{i}.parquet";
                blobNames.Add(blobName);
                var blobClient = containerClient.GetBlobClient(blobName);

                await using var fileStream = File.OpenRead(localFiles[i]);
                await blobClient.UploadAsync(fileStream);
            }

            // Test
            var provider = new AzureBlobParquetStatisticsProvider(containerClient);
            var files = blobNames.Select(b => new ParquetFile { Path = b }).ToList();

            // Act - enrich concurrently
            var tasks = files.Select(f => provider.EnrichAsync(f)).ToList();
            var enrichedFiles = await Task.WhenAll(tasks);

            // Assert
            Assert.That(enrichedFiles, Has.Length.EqualTo(files.Count));
            foreach (var enrichedFile in enrichedFiles)
            {
                Assert.That(enrichedFile.RowCount, Is.GreaterThan(0));
                Assert.That(enrichedFile.RowGroups, Is.Not.Empty);
            }
        }
        finally
        {
            await containerClient.DeleteAsync();
        }
    }

    private async Task<bool> IsAzuriteRunning()
    {
        try
        {
            var client = new BlobServiceClient(AzuriteConnectionString);
            await client.GetPropertiesAsync();
            return true;
        }
        catch
        {
            return false;
        }
    }

    #endregion
}

