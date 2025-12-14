using Azure.Storage.Blobs;
using ParquetSharpLINQ.Azure;
using ParquetSharpLINQ.DataGenerator;
using ParquetSharpLINQ.Models;
using ParquetSharpLINQ.Tests.Integration.Helpers;

namespace ParquetSharpLINQ.Tests.Integration.Azure;

[TestFixture]
[Category("Integration")]
[Category("Azure")]
public class AzureStatisticsProviderTests
{
    private string _localTestDataPath = null!;

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
    }

    [OneTimeTearDown]
    public void OneTimeTearDown()
    {
        if (Directory.Exists(_localTestDataPath))
        {
            Directory.Delete(_localTestDataPath, true);
        }
    }

    [Test]
    public async Task EnrichAsync_IntegratesWithBlobStorage()
    {
        BlobContainerClient? containerClient = null;
        try
        {
            containerClient = await AzuriteTestHelper.CreateContainerAsync("test-stats");

            var localFiles = Directory.GetFiles(_localTestDataPath, "*.parquet", SearchOption.AllDirectories);
            var testFile = localFiles.First();
            var blobName = "test/data.parquet";
            var blobClient = containerClient.GetBlobClient(blobName);

            await using (var fileStream = File.OpenRead(testFile))
            {
                await blobClient.UploadAsync(fileStream);
            }

            var provider = new AzureBlobParquetStatisticsProvider(containerClient);
            var file = new ParquetFile { Path = blobName };

            var enrichedFile = await provider.EnrichAsync(file);

            Assert.That(enrichedFile.Path, Is.EqualTo(blobName));
            Assert.That(enrichedFile.SizeBytes, Is.GreaterThan(0), "Should read blob size");
            Assert.That(enrichedFile.RowCount, Is.GreaterThan(0), "Should extract from blob");
            Assert.That(enrichedFile.RowGroups, Is.Not.Empty, "Should use AzureBlobRangeStream");
            Assert.That(enrichedFile.RowGroups[0].ColumnStatisticsByPath, Is.Not.Empty);
        }
        catch (InvalidOperationException ex)
        {
            Assert.Inconclusive(ex.Message);
        }
        finally
        {
            await AzuriteTestHelper.DeleteContainerAsync(containerClient);
        }
    }

    [Test]
    public async Task ConcurrentEnrichment_ThreadSafe()
    {
        BlobContainerClient? containerClient = null;
        try
        {
            containerClient = await AzuriteTestHelper.CreateContainerAsync("test-stats");

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

            var provider = new AzureBlobParquetStatisticsProvider(containerClient);
            var files = blobNames.Select(b => new ParquetFile { Path = b }).ToList();

            var tasks = files.Select(f => provider.EnrichAsync(f)).ToList();
            var enrichedFiles = await Task.WhenAll(tasks);

            Assert.That(enrichedFiles, Has.Length.EqualTo(files.Count));
            foreach (var enrichedFile in enrichedFiles)
            {
                Assert.That(enrichedFile.RowCount, Is.GreaterThan(0));
                Assert.That(enrichedFile.RowGroups, Is.Not.Empty);
            }
        }
        catch (InvalidOperationException ex)
        {
            Assert.Ignore(ex.Message);
        }
        finally
        {
            await AzuriteTestHelper.DeleteContainerAsync(containerClient);
        }
    }
}

