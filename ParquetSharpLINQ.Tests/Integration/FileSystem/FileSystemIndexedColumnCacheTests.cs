using ParquetSharpLINQ.DataGenerator;
using ParquetSharpLINQ.Tests.Integration.Helpers;
using ParquetSharpLINQ.Tests.Integration.IndexedColumnCache;

namespace ParquetSharpLINQ.Tests.Integration.FileSystem;

[TestFixture]
[Category("Integration")]
[Category("LocalFiles")]
public class FileSystemIndexedColumnCacheTests : IndexedColumnCacheTestsBase
{
    private string _testDataPath = null!;
    private TrackingParquetReader _trackingReader = null!;

    [SetUp]
    public void SetUp()
    {
        _testDataPath = Path.Combine(Path.GetTempPath(), $"ParquetIndexedCache_{Guid.NewGuid()}");
        Directory.CreateDirectory(_testDataPath);

        var generator = new TestDataGenerator();
        generator.GenerateParquetFiles(
            _testDataPath,
            recordsPerPartition: 20,
            years: [2024],
            monthsPerYear: 1,
            rowGroupsPerFile: 2
        );

        _trackingReader = new TrackingParquetReader();
    }

    [TearDown]
    public void TearDown()
    {
        if (Directory.Exists(_testDataPath))
            Directory.Delete(_testDataPath, true);
    }

    protected override ParquetTable<IndexedSalesRecord> CreateTable()
    {
        return ParquetTable<IndexedSalesRecord>.Factory.FromFileSystem(
            _testDataPath,
            reader: _trackingReader);
    }

    protected override int GetIndexReadCount(string columnName)
    {
        return _trackingReader.GetIndexReadCount(columnName);
    }
}
