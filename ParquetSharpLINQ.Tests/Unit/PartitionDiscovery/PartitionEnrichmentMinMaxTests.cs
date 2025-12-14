using ParquetSharpLINQ.Discovery;
using ParquetSharpLINQ.Statistics;

namespace ParquetSharpLINQ.Tests.Unit.PartitionDiscovery;

[TestFixture]
[Category("Unit")]
[Category("PartitionDiscovery")]
public class PartitionEnrichmentMinMaxTests
{
    private string _testDirectory = null!;
    private ParquetStatisticsExtractor _extractor = null!;

    [SetUp]
    public void Setup()
    {
        _testDirectory = Path.Combine(Path.GetTempPath(), $"ParquetStatsTest_{Guid.NewGuid()}");
        Directory.CreateDirectory(_testDirectory);
        _extractor = new ParquetStatisticsExtractor();
    }

    [TearDown]
    public void TearDown()
    {
        if (Directory.Exists(_testDirectory))
        {
            Directory.Delete(_testDirectory, true);
        }
    }

    [Test]
    public void FileSystemDiscovery_WithStatisticsProvider_VerifiesIntegerMinMax()
    {
        // Arrange
        var partition = Path.Combine(_testDirectory, "year=2024");
        Directory.CreateDirectory(partition);
        ParquetTestFileHelper.CreateTestParquetFileWithIntegers(partition, "data.parquet", 
            new[] { -50, 100, 25, 0, -100, 999 });

        var statisticsProvider = new FileSystemParquetStatisticsProvider(_extractor);
        var discovery = new FileSystemPartitionDiscovery(
            _testDirectory,
            statisticsProvider: statisticsProvider);

        // Act
        var partitions = discovery.DiscoverPartitions().ToList();

        // Assert
        var file = partitions[0].Files[0];
        var stats = file.RowGroups[0].ColumnStatisticsByPath["int_value"];

        Assert.That(stats.HasMinMax, Is.True);
        var success = stats.TryGetMinMax<int>(out var min, out var max);
        Assert.That(success, Is.True, "Should decode integer min/max");
        Assert.That(min, Is.EqualTo(-100), "Min should be exactly -100");
        Assert.That(max, Is.EqualTo(999), "Max should be exactly 999");
    }

    [Test]
    public void FileSystemDiscovery_WithStatisticsProvider_VerifiesLongMinMax()
    {
        // Arrange
        var partition = Path.Combine(_testDirectory, "year=2024");
        Directory.CreateDirectory(partition);
        ParquetTestFileHelper.CreateTestParquetFileWithLongs(partition, "data.parquet", 
            new[] { 1000L, 5000L, 2500L, -1000L, 10000L });

        var statisticsProvider = new FileSystemParquetStatisticsProvider(_extractor);
        var discovery = new FileSystemPartitionDiscovery(
            _testDirectory,
            statisticsProvider: statisticsProvider);

        // Act
        var partitions = discovery.DiscoverPartitions().ToList();

        // Assert
        var file = partitions[0].Files[0];
        var stats = file.RowGroups[0].ColumnStatisticsByPath["long_value"];

        Assert.That(stats.HasMinMax, Is.True);
        var success = stats.TryGetMinMax<long>(out var min, out var max);
        Assert.That(success, Is.True, "Should decode long min/max");
        Assert.That(min, Is.EqualTo(-1000L), "Min should be exactly -1000");
        Assert.That(max, Is.EqualTo(10000L), "Max should be exactly 10000");
    }

    [Test]
    public void FileSystemDiscovery_WithStatisticsProvider_VerifiesDoubleMinMax()
    {
        // Arrange
        var partition = Path.Combine(_testDirectory, "year=2024");
        Directory.CreateDirectory(partition);
        ParquetTestFileHelper.CreateTestParquetFileWithDoubles(partition, "data.parquet", 
            new[] { -123.456, 0.0, 789.012, 3.14159, -999.99 });

        var statisticsProvider = new FileSystemParquetStatisticsProvider(_extractor);
        var discovery = new FileSystemPartitionDiscovery(
            _testDirectory,
            statisticsProvider: statisticsProvider);

        // Act
        var partitions = discovery.DiscoverPartitions().ToList();

        // Assert
        var file = partitions[0].Files[0];
        var stats = file.RowGroups[0].ColumnStatisticsByPath["double_value"];

        Assert.That(stats.HasMinMax, Is.True);
        var success = stats.TryGetMinMax<double>(out var min, out var max);
        Assert.That(success, Is.True, "Should decode double min/max");
        Assert.That(min, Is.EqualTo(-999.99).Within(0.000001), "Min should be exactly -999.99");
        Assert.That(max, Is.EqualTo(789.012).Within(0.000001), "Max should be exactly 789.012");
    }

    [Test]
    public void FileSystemDiscovery_WithStatisticsProvider_VerifiesStringMinMax()
    {
        // Arrange
        var partition = Path.Combine(_testDirectory, "year=2024");
        Directory.CreateDirectory(partition);
        ParquetTestFileHelper.CreateTestParquetFileWithStrings(partition, "data.parquet", 
            new[] { "zebra", "apple", "mango", "aardvark", "zoo" });

        var statisticsProvider = new FileSystemParquetStatisticsProvider(_extractor);
        var discovery = new FileSystemPartitionDiscovery(
            _testDirectory,
            statisticsProvider: statisticsProvider);

        // Act
        var partitions = discovery.DiscoverPartitions().ToList();

        // Assert
        var file = partitions[0].Files[0];
        var stats = file.RowGroups[0].ColumnStatisticsByPath["string_value"];

        if (stats.HasMinMax)
        {
            var success = stats.TryGetMinMax<string>(out var min, out var max);
            Assert.That(success, Is.True, "Should decode string min/max");
            Assert.That(min, Is.EqualTo("aardvark"), "Min should be 'aardvark' (lexicographically first)");
            Assert.That(max, Is.EqualTo("zoo"), "Max should be 'zoo' (lexicographically last)");
        }
    }

    [Test]
    public void FileSystemDiscovery_WithStatisticsProvider_VerifiesDateOnlyMinMax()
    {
        // Arrange
        var partition = Path.Combine(_testDirectory, "year=2024");
        Directory.CreateDirectory(partition);
        ParquetTestFileHelper.CreateTestParquetFileWithDateOnly(partition, "data.parquet", 
            new[]
            {
                new DateOnly(2024, 12, 31),
                new DateOnly(2024, 1, 1),
                new DateOnly(2024, 6, 15),
                new DateOnly(2024, 3, 20)
            });

        var statisticsProvider = new FileSystemParquetStatisticsProvider(_extractor);
        var discovery = new FileSystemPartitionDiscovery(
            _testDirectory,
            statisticsProvider: statisticsProvider);

        // Act
        var partitions = discovery.DiscoverPartitions().ToList();

        // Assert
        var file = partitions[0].Files[0];
        var stats = file.RowGroups[0].ColumnStatisticsByPath["date_value"];

        Assert.That(stats.HasMinMax, Is.True);
        var success = stats.TryGetMinMax<DateOnly>(out var min, out var max);
        Assert.That(success, Is.True, "Should decode DateOnly min/max");
        Assert.That(min, Is.EqualTo(new DateOnly(2024, 1, 1)), "Min date should be 2024-01-01");
        Assert.That(max, Is.EqualTo(new DateOnly(2024, 12, 31)), "Max date should be 2024-12-31");
    }

    [Test]
    public void FileSystemDiscovery_WithStatisticsProvider_VerifiesMinMaxAcrossMultipleFiles()
    {
        // Arrange
        var partition = Path.Combine(_testDirectory, "year=2024");
        Directory.CreateDirectory(partition);
        
        ParquetTestFileHelper.CreateTestParquetFileWithIntegers(partition, "file1.parquet", new[] { 1, 10, 5 });
        ParquetTestFileHelper.CreateTestParquetFileWithIntegers(partition, "file2.parquet", new[] { 20, 30, 25 });
        ParquetTestFileHelper.CreateTestParquetFileWithIntegers(partition, "file3.parquet", new[] { -5, 0, 2 });

        var statisticsProvider = new FileSystemParquetStatisticsProvider(_extractor);
        var discovery = new FileSystemPartitionDiscovery(
            _testDirectory,
            statisticsProvider: statisticsProvider);

        // Act
        var partitions = discovery.DiscoverPartitions().ToList();

        // Assert
        var files = partitions[0].Files.ToList();
        Assert.That(files, Has.Count.EqualTo(3));

        var file1 = files.First(f => f.Path.EndsWith("file1.parquet"));
        var stats1 = file1.RowGroups[0].ColumnStatisticsByPath["int_value"];
        stats1.TryGetMinMax<int>(out var min1, out var max1);
        Assert.That(min1, Is.EqualTo(1), "File1 min should be 1");
        Assert.That(max1, Is.EqualTo(10), "File1 max should be 10");

        var file2 = files.First(f => f.Path.EndsWith("file2.parquet"));
        var stats2 = file2.RowGroups[0].ColumnStatisticsByPath["int_value"];
        stats2.TryGetMinMax<int>(out var min2, out var max2);
        Assert.That(min2, Is.EqualTo(20), "File2 min should be 20");
        Assert.That(max2, Is.EqualTo(30), "File2 max should be 30");

        var file3 = files.First(f => f.Path.EndsWith("file3.parquet"));
        var stats3 = file3.RowGroups[0].ColumnStatisticsByPath["int_value"];
        stats3.TryGetMinMax<int>(out var min3, out var max3);
        Assert.That(min3, Is.EqualTo(-5), "File3 min should be -5");
        Assert.That(max3, Is.EqualTo(2), "File3 max should be 2");
    }

    [Test]
    public void FileSystemDiscovery_WithStatisticsProvider_VerifiesMinMaxPerRowGroup()
    {
        // Arrange
        var partition = Path.Combine(_testDirectory, "year=2024");
        Directory.CreateDirectory(partition);
        
        var path = Path.Combine(partition, "multirowgroup.parquet");
        ParquetTestFileHelper.CreateParquetFileWithThreeRowGroups(path);

        var statisticsProvider = new FileSystemParquetStatisticsProvider(_extractor);
        var discovery = new FileSystemPartitionDiscovery(
            _testDirectory,
            statisticsProvider: statisticsProvider);

        // Act
        var partitions = discovery.DiscoverPartitions().ToList();

        // Assert
        var file = partitions[0].Files[0];
        Assert.That(file.RowGroups, Has.Count.EqualTo(3));

        var rg1Stats = file.RowGroups[0].ColumnStatisticsByPath["value"];
        rg1Stats.TryGetMinMax<int>(out var rg1Min, out var rg1Max);
        Assert.That(rg1Min, Is.EqualTo(1), "RG1 min should be 1");
        Assert.That(rg1Max, Is.EqualTo(5), "RG1 max should be 5");

        var rg2Stats = file.RowGroups[1].ColumnStatisticsByPath["value"];
        rg2Stats.TryGetMinMax<int>(out var rg2Min, out var rg2Max);
        Assert.That(rg2Min, Is.EqualTo(10), "RG2 min should be 10");
        Assert.That(rg2Max, Is.EqualTo(20), "RG2 max should be 20");

        var rg3Stats = file.RowGroups[2].ColumnStatisticsByPath["value"];
        rg3Stats.TryGetMinMax<int>(out var rg3Min, out var rg3Max);
        Assert.That(rg3Min, Is.EqualTo(-10), "RG3 min should be -10");
        Assert.That(rg3Max, Is.EqualTo(0), "RG3 max should be 0");
    }

    [Test]
    public void FileSystemDiscovery_WithStatisticsProvider_VerifiesMinMaxAcrossMultiplePartitions()
    {
        // Arrange
        var partition1 = Path.Combine(_testDirectory, "year=2023");
        Directory.CreateDirectory(partition1);
        ParquetTestFileHelper.CreateTestParquetFileWithIntegers(partition1, "data.parquet", new[] { 100, 200, 150 });

        var partition2 = Path.Combine(_testDirectory, "year=2024");
        Directory.CreateDirectory(partition2);
        ParquetTestFileHelper.CreateTestParquetFileWithIntegers(partition2, "data.parquet", new[] { -50, 0, 25 });

        var statisticsProvider = new FileSystemParquetStatisticsProvider(_extractor);
        var discovery = new FileSystemPartitionDiscovery(
            _testDirectory,
            statisticsProvider: statisticsProvider);

        // Act
        var partitions = discovery.DiscoverPartitions().ToList();

        // Assert
        Assert.That(partitions, Has.Count.EqualTo(2));

        var p2023 = partitions.First(p => p.Values["year"] == "2023");
        var stats2023 = p2023.Files[0].RowGroups[0].ColumnStatisticsByPath["int_value"];
        stats2023.TryGetMinMax<int>(out var min2023, out var max2023);
        Assert.That(min2023, Is.EqualTo(100), "2023 partition min should be 100");
        Assert.That(max2023, Is.EqualTo(200), "2023 partition max should be 200");

        var p2024 = partitions.First(p => p.Values["year"] == "2024");
        var stats2024 = p2024.Files[0].RowGroups[0].ColumnStatisticsByPath["int_value"];
        stats2024.TryGetMinMax<int>(out var min2024, out var max2024);
        Assert.That(min2024, Is.EqualTo(-50), "2024 partition min should be -50");
        Assert.That(max2024, Is.EqualTo(25), "2024 partition max should be 25");
    }

    [Test]
    public void FileSystemDiscovery_WithStatisticsProvider_VerifiesRawByteEncoding()
    {
        // Arrange
        var partition = Path.Combine(_testDirectory, "year=2024");
        Directory.CreateDirectory(partition);
        ParquetTestFileHelper.CreateTestParquetFileWithIntegers(partition, "data.parquet", new[] { -100, 50, 999 });

        var statisticsProvider = new FileSystemParquetStatisticsProvider(_extractor);
        var discovery = new FileSystemPartitionDiscovery(
            _testDirectory,
            statisticsProvider: statisticsProvider);

        // Act
        var partitions = discovery.DiscoverPartitions().ToList();

        // Assert
        var file = partitions[0].Files[0];
        var stats = file.RowGroups[0].ColumnStatisticsByPath["int_value"];

        Assert.That(stats.MinRaw, Is.Not.Null, "Should have raw min bytes");
        Assert.That(stats.MaxRaw, Is.Not.Null, "Should have raw max bytes");

        var minFromBytes = BitConverter.ToInt32(stats.MinRaw!, 0);
        var maxFromBytes = BitConverter.ToInt32(stats.MaxRaw!, 0);

        Assert.That(minFromBytes, Is.EqualTo(-100), "Raw min bytes should decode to -100");
        Assert.That(maxFromBytes, Is.EqualTo(999), "Raw max bytes should decode to 999");
    }
}