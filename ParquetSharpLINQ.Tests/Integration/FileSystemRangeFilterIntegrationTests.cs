using ParquetSharp;

namespace ParquetSharpLINQ.Tests.Integration;

/// <summary>
/// Range filter integration tests for local file system storage.
/// </summary>
[TestFixture]
[Category("Integration")]
[Category("LocalFiles")]
public class FileSystemRangeFilterIntegrationTests : RangeFilterIntegrationTestsBase
{
    private string _testDirectory = null!;
    private TrackingParquetReader _trackingReader = null!;

    protected override IReadOnlySet<string> FilesRead => _trackingReader.FilesRead;

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
        return ParquetTable<T>.Factory.FromFileSystem(
            _testDirectory,
            reader: _trackingReader);
    }

    protected virtual string GetFilePath(string fileName)
    {
        return Path.Combine(_testDirectory, fileName);
    }

    protected virtual void SetupTestEnvironment()
    {
        _testDirectory = Path.Combine(Path.GetTempPath(), $"RangeFilterIntegration_{Guid.NewGuid()}");
        Directory.CreateDirectory(_testDirectory);
        _trackingReader = new TrackingParquetReader();
    }

    protected virtual void CleanupTestEnvironment()
    {
        if (Directory.Exists(_testDirectory))
        {
            Directory.Delete(_testDirectory, true);
        }
    }

    protected override void CreateFileWithIntRange(string fileName, int minValue, int maxValue)
    {
        var path = GetFilePath(fileName);
        var columns = new Column[]
        {
            new Column<long>("id"),
            new Column<int>("value")
        };

        using var writer = new ParquetFileWriter(path, columns);
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

    protected override void CreateFileWithDateOnlyRange(string fileName, DateOnly minDate, DateOnly maxDate)
    {
        var path = GetFilePath(fileName);
        var columns = new Column[]
        {
            new Column<long>("id"),
            new Column<DateOnly>("event_date", LogicalType.Date())
        };

        using var writer = new ParquetFileWriter(path, columns);
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
}

