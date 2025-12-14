using Azure.Storage.Blobs;
using ParquetSharp;
using ParquetSharpLINQ.Azure;
using ParquetSharpLINQ.Tests.Integration.Helpers;
using ParquetSharpLINQ.Tests.Integration.RangeFilter;

namespace ParquetSharpLINQ.Tests.Integration.Azure;

/// <summary>
/// Range filter integration tests for Azure Blob Storage.
/// Requires Azurite to be running on localhost:10000.
/// </summary>
[TestFixture]
[Category("Integration")]
[Category("Azure")]
public class AzureRangeFilterTests : RangeFilterTestsBase
{
    private BlobContainerClient? _containerClient;
    private string _localTempDir = null!;
    private TrackingAzureBlobReader _azureTrackingReader = null!;

    protected override IReadOnlySet<string> FilesRead => _azureTrackingReader.FilesRead;

    private BlobContainerClient ContainerClient => 
        _containerClient ?? throw new InvalidOperationException("ContainerClient is not initialized");

    [SetUp]
    public void Setup()
    {
        try
        {
            _containerClient = AzuriteTestHelper.CreateContainerAsync("rangefilter").GetAwaiter().GetResult();

            // Create Azure-specific tracking reader
            _azureTrackingReader = new TrackingAzureBlobReader(_containerClient);

            // Create local temp directory for file creation
            _localTempDir = Path.Combine(Path.GetTempPath(), $"AzureRangeFilter_{Guid.NewGuid()}");
            Directory.CreateDirectory(_localTempDir);
        }
        catch (InvalidOperationException ex)
        {
            Assert.Inconclusive(ex.Message);
        }
    }

    [TearDown]
    public void TearDown()
    {
        AzuriteTestHelper.DeleteContainerAsync(_containerClient).GetAwaiter().GetResult();
        TestDataHelper.CleanupDirectory(_localTempDir);
    }

    protected override ParquetTable<T> CreateTable<T>()
    {
        return ParquetTable<T>.Factory.FromAzureBlob(
            ContainerClient,
            reader: _azureTrackingReader);
    }

    private string GetFilePath(string fileName)
    {
        // For Azure, we create files locally first, then upload
        return Path.Combine(_localTempDir, fileName);
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
        var blobClient = ContainerClient.GetBlobClient(fileName);
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
        var blobClient = ContainerClient.GetBlobClient(fileName);
        using var fileStream = File.OpenRead(localPath);
        blobClient.Upload(fileStream, overwrite: true);
    }
}

