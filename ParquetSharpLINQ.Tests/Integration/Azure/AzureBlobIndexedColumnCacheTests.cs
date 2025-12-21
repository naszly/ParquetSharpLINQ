using Azure.Storage.Blobs;
using ParquetSharpLINQ.Azure;
using ParquetSharpLINQ.DataGenerator;
using ParquetSharpLINQ.Tests.Integration.Helpers;
using ParquetSharpLINQ.Tests.Integration.IndexedColumnCache;

namespace ParquetSharpLINQ.Tests.Integration.Azure;

[TestFixture]
[Category("Integration")]
[Category("Azure")]
public class AzureBlobIndexedColumnCacheTests : IndexedColumnCacheTestsBase
{
    private BlobContainerClient? _containerClient;
    private string _localTempDir = null!;
    private TrackingAzureBlobReader _trackingReader = null!;

    private BlobContainerClient ContainerClient =>
        _containerClient ?? throw new InvalidOperationException("ContainerClient is not initialized");

    [SetUp]
    public async Task SetUp()
    {
        try
        {
            _containerClient = await AzuriteTestHelper.CreateContainerAsync("indexed-cache");
            _trackingReader = new TrackingAzureBlobReader(ContainerClient);

            _localTempDir = Path.Combine(Path.GetTempPath(), $"AzureIndexedCache_{Guid.NewGuid()}");
            Directory.CreateDirectory(_localTempDir);

            await UploadTestDataAsync();
        }
        catch (InvalidOperationException ex)
        {
            Assert.Inconclusive(ex.Message);
        }
    }

    [TearDown]
    public async Task TearDown()
    {
        await AzuriteTestHelper.DeleteContainerAsync(_containerClient);
        TestDataHelper.CleanupDirectory(_localTempDir);
    }

    protected override ParquetTable<IndexedSalesRecord> CreateTable()
    {
        return ParquetTable<IndexedSalesRecord>.Factory.FromAzureBlob(
            ContainerClient,
            reader: _trackingReader);
    }

    protected override int GetIndexReadCount(string columnName)
    {
        return _trackingReader.GetIndexReadCount(columnName);
    }

    private async Task UploadTestDataAsync()
    {
        var generator = new TestDataGenerator();
        generator.GenerateParquetFiles(
            _localTempDir,
            recordsPerPartition: 20,
            years: [2024],
            monthsPerYear: 1,
            rowGroupsPerFile: 2
        );

        var files = Directory.GetFiles(_localTempDir, "*.parquet", SearchOption.AllDirectories);

        foreach (var file in files)
        {
            var relativePath = Path.GetRelativePath(_localTempDir, file).Replace('\\', '/');
            var blobClient = ContainerClient.GetBlobClient(relativePath);

            await using var stream = File.OpenRead(file);
            await blobClient.UploadAsync(stream, true);
        }
    }
}
