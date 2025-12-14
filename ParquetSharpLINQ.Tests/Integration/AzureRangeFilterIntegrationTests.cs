using Azure.Storage.Blobs;
using ParquetSharp;
using ParquetSharpLINQ.Azure;

namespace ParquetSharpLINQ.Tests.Integration;

/// <summary>
/// Range filter integration tests for Azure Blob Storage.
/// Requires Azurite to be running on localhost:10000.
/// </summary>
[TestFixture]
[Category("Integration")]
[Category("Azure")]
public class AzureRangeFilterIntegrationTests : RangeFilterIntegrationTestsBase
{
    private const string AzuriteConnectionString =
        "DefaultEndpointsProtocol=http;" +
        "AccountName=devstoreaccount1;" +
        "AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;" +
        "BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;";

    private string _containerName = null!;
    private BlobContainerClient _containerClient = null!;
    private string _localTempDir = null!;
    private TrackingAzureBlobReader _azureTrackingReader = null!;

    protected override IReadOnlySet<string> FilesRead => _azureTrackingReader.FilesRead;

    [SetUp]
    public void Setup()
    {
        SetupTestEnvironment();
    }

    [TearDown]
    public void TearDown()
    {
        CleanupTestEnvironment();
    }

    protected override ParquetTable<T> CreateTable<T>()
    {
        return ParquetTable<T>.Factory.FromAzureBlob(
            _containerClient,
            reader: _azureTrackingReader);
    }

    private string GetFilePath(string fileName)
    {
        // For Azure, we create files locally first, then upload
        return Path.Combine(_localTempDir, fileName);
    }

    private void SetupTestEnvironment()
    {
        // Check if Azurite is running
        if (!IsAzuriteRunning().Result)
        {
            Assert.Ignore("Azurite not running. Start with: docker run -p 10000:10000 mcr.microsoft.com/azure-storage/azurite");
        }

        // Create unique container for this test run
        _containerName = $"rangefilter-{Guid.NewGuid():N}";
        var serviceClient = new BlobServiceClient(AzuriteConnectionString);
        _containerClient = serviceClient.GetBlobContainerClient(_containerName);
        _containerClient.CreateAsync().GetAwaiter().GetResult();

        // Create Azure-specific tracking reader
        _azureTrackingReader = new TrackingAzureBlobReader(_containerClient);

        // Create local temp directory for file creation
        _localTempDir = Path.Combine(Path.GetTempPath(), $"AzureRangeFilter_{Guid.NewGuid()}");
        Directory.CreateDirectory(_localTempDir);
    }

    private void CleanupTestEnvironment()
    {
        // Delete Azure container
        if (_containerClient != null)
        {
            try
            {
                _containerClient.DeleteAsync().Wait();
            }
            catch
            {
                // Ignore cleanup errors
            }
        }

        // Delete local temp directory
        if (Directory.Exists(_localTempDir))
        {
            try
            {
                Directory.Delete(_localTempDir, true);
            }
            catch
            {
                // Ignore cleanup errors
            }
        }
    }

    protected override void CreateFileWithIntRange(string fileName, int minValue, int maxValue)
    {
        // Create file locally using base implementation
        var localPath = GetFilePath(fileName);
        var columns = new Column[]
        {
            new Column<long>("id"),
            new Column<int>("value")
        };

        using (var writer = new ParquetFileWriter(localPath, columns))
        {
            using var rowGroup = writer.AppendRowGroup();

            var count = maxValue - minValue + 1;
            var ids = Enumerable.Range(minValue, count).Select(i => (long)i).ToArray();
            var values = Enumerable.Range(minValue, count).ToArray();

            using (var idWriter = rowGroup.NextColumn().LogicalWriter<long>())
            {
                idWriter.WriteBatch(ids);
            }
            using (var valueWriter = rowGroup.NextColumn().LogicalWriter<int>())
            {
                valueWriter.WriteBatch(values);
            }
        }

        // Upload to Azure
        var blobClient = _containerClient.GetBlobClient(fileName);
        using var fileStream = File.OpenRead(localPath);
        blobClient.Upload(fileStream, overwrite: true);
    }

    protected override void CreateFileWithDateOnlyRange(string fileName, DateOnly minDate, DateOnly maxDate)
    {
        // Create file locally
        var localPath = GetFilePath(fileName);
        var columns = new Column[]
        {
            new Column<long>("id"),
            new Column<DateOnly>("event_date", LogicalType.Date())
        };

        using (var writer = new ParquetFileWriter(localPath, columns))
        {
            using var rowGroup = writer.AppendRowGroup();

            var days = maxDate.DayNumber - minDate.DayNumber + 1;
            var ids = Enumerable.Range(1, days).Select(i => (long)i).ToArray();
            var dates = Enumerable.Range(0, days).Select(i => minDate.AddDays(i)).ToArray();

            using (var idWriter = rowGroup.NextColumn().LogicalWriter<long>())
            {
                idWriter.WriteBatch(ids);
            }
            using (var dateWriter = rowGroup.NextColumn().LogicalWriter<DateOnly>())
            {
                dateWriter.WriteBatch(dates);
            }
        }

        // Upload to Azure
        var blobClient = _containerClient.GetBlobClient(fileName);
        using var fileStream = File.OpenRead(localPath);
        blobClient.Upload(fileStream, overwrite: true);
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
}

