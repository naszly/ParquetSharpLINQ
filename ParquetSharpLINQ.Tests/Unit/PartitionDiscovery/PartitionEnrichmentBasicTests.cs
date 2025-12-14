namespace ParquetSharpLINQ.Tests.Unit.PartitionDiscovery;

[TestFixture]
[Category("Unit")]
[Category("PartitionDiscovery")]
public class PartitionEnrichmentBasicTests
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
    public void FileSystemDiscovery_WithStatisticsProvider_EnrichesPartitions()
    {
        // Arrange
        var partition = Path.Combine(_testDirectory, "year=2024");
        Directory.CreateDirectory(partition);
        ParquetTestFileHelper.CreateTestParquetFile(partition, "data.parquet", new[] { 1, 2, 3, 4, 5 });

        var statisticsProvider = new FileSystemParquetStatisticsProvider(_extractor);
        var discovery = new FileSystemPartitionDiscovery(
            _testDirectory,
            statisticsProvider: statisticsProvider);

        // Act
        var partitions = discovery.DiscoverPartitions().ToList();

        // Assert
        Assert.That(partitions, Has.Count.EqualTo(1));
        Assert.That(partitions[0].Files, Has.Count.EqualTo(1));
        
        var file = partitions[0].Files[0];
        Assert.That(file.RowCount, Is.EqualTo(5), "Should have enriched row count");
        Assert.That(file.SizeBytes, Is.GreaterThan(0), "Should have enriched size");
        Assert.That(file.RowGroups, Is.Not.Empty, "Should have enriched row groups");
    }

    [Test]
    public void FileSystemDiscovery_WithStatisticsProvider_EnrichesMultipleFiles()
    {
        // Arrange
        var partition = Path.Combine(_testDirectory, "year=2024");
        Directory.CreateDirectory(partition);
        ParquetTestFileHelper.CreateTestParquetFile(partition, "file1.parquet", new[] { 1, 2, 3 });
        ParquetTestFileHelper.CreateTestParquetFile(partition, "file2.parquet", new[] { 4, 5, 6 });
        ParquetTestFileHelper.CreateTestParquetFile(partition, "file3.parquet", new[] { 7, 8, 9, 10 });

        var statisticsProvider = new FileSystemParquetStatisticsProvider(_extractor);
        var discovery = new FileSystemPartitionDiscovery(
            _testDirectory,
            statisticsProvider: statisticsProvider);

        // Act
        var partitions = discovery.DiscoverPartitions().ToList();

        // Assert
        Assert.That(partitions, Has.Count.EqualTo(1));
        Assert.That(partitions[0].Files, Has.Count.EqualTo(3));
        
        foreach (var file in partitions[0].Files)
        {
            Assert.That(file.RowCount, Is.GreaterThan(0));
            Assert.That(file.SizeBytes, Is.GreaterThan(0));
            Assert.That(file.RowGroups, Is.Not.Empty);
        }

        var file1 = partitions[0].Files.First(f => f.Path.EndsWith("file1.parquet"));
        Assert.That(file1.RowCount, Is.EqualTo(3));
        
        var file2 = partitions[0].Files.First(f => f.Path.EndsWith("file2.parquet"));
        Assert.That(file2.RowCount, Is.EqualTo(3));
        
        var file3 = partitions[0].Files.First(f => f.Path.EndsWith("file3.parquet"));
        Assert.That(file3.RowCount, Is.EqualTo(4));
    }

    [Test]
    public void FileSystemDiscovery_WithStatisticsProvider_EnrichesMultiplePartitions()
    {
        // Arrange
        var partition1 = Path.Combine(_testDirectory, "year=2023");
        Directory.CreateDirectory(partition1);
        ParquetTestFileHelper.CreateTestParquetFile(partition1, "data.parquet", new[] { 1, 2, 3 });

        var partition2 = Path.Combine(_testDirectory, "year=2024");
        Directory.CreateDirectory(partition2);
        ParquetTestFileHelper.CreateTestParquetFile(partition2, "data.parquet", new[] { 4, 5, 6, 7 });

        var statisticsProvider = new FileSystemParquetStatisticsProvider(_extractor);
        var discovery = new FileSystemPartitionDiscovery(
            _testDirectory,
            statisticsProvider: statisticsProvider);

        // Act
        var partitions = discovery.DiscoverPartitions().ToList();

        // Assert
        Assert.That(partitions, Has.Count.EqualTo(2));
        
        var p2023 = partitions.First(p => p.Values["year"] == "2023");
        Assert.That(p2023.Files[0].RowCount, Is.EqualTo(3));
        
        var p2024 = partitions.First(p => p.Values["year"] == "2024");
        Assert.That(p2024.Files[0].RowCount, Is.EqualTo(4));
    }

    [Test]
    public void FileSystemDiscovery_WithoutStatisticsProvider_DoesNotEnrich()
    {
        // Arrange
        var partition = Path.Combine(_testDirectory, "year=2024");
        Directory.CreateDirectory(partition);
        ParquetTestFileHelper.CreateTestParquetFile(partition, "data.parquet", new[] { 1, 2, 3 });

        var discovery = new FileSystemPartitionDiscovery(_testDirectory);

        // Act
        var partitions = discovery.DiscoverPartitions().ToList();

        // Assert
        Assert.That(partitions, Has.Count.EqualTo(1));
        Assert.That(partitions[0].Files, Has.Count.EqualTo(1));
        
        var file = partitions[0].Files[0];
        Assert.That(file.RowCount, Is.Null, "Should not have enriched row count");
        Assert.That(file.SizeBytes, Is.Null, "Should not have enriched size");
        Assert.That(file.RowGroups, Is.Empty, "Should not have enriched row groups");
    }

    [Test]
    public void FileSystemDiscovery_WithStatisticsProvider_ExtractsColumnStatistics()
    {
        // Arrange
        var partition = Path.Combine(_testDirectory, "year=2024");
        Directory.CreateDirectory(partition);
        ParquetTestFileHelper.CreateTestParquetFileWithMultipleColumns(partition, "data.parquet");

        var statisticsProvider = new FileSystemParquetStatisticsProvider(_extractor);
        var discovery = new FileSystemPartitionDiscovery(
            _testDirectory,
            statisticsProvider: statisticsProvider);

        // Act
        var partitions = discovery.DiscoverPartitions().ToList();

        // Assert
        var file = partitions[0].Files[0];
        var rowGroup = file.RowGroups[0];
        
        Assert.That(rowGroup.ColumnStatisticsByPath, Is.Not.Empty);
        Assert.That(rowGroup.ColumnStatisticsByPath.ContainsKey("id"), Is.True);
        Assert.That(rowGroup.ColumnStatisticsByPath.ContainsKey("value"), Is.True);
        Assert.That(rowGroup.ColumnStatisticsByPath.ContainsKey("name"), Is.True);

        var idStats = rowGroup.ColumnStatisticsByPath["id"];
        Assert.That(idStats.HasMinMax, Is.True);
        var success = idStats.TryGetMinMax<long>(out var min, out var max);
        Assert.That(success, Is.True);
        Assert.That(min, Is.EqualTo(1));
        Assert.That(max, Is.EqualTo(5));
    }

    [Test]
    public void FileSystemDiscovery_WithStatisticsProvider_HandlesMultipleRowGroups()
    {
        // Arrange
        var partition = Path.Combine(_testDirectory, "year=2024");
        Directory.CreateDirectory(partition);
        ParquetTestFileHelper.CreateTestParquetFileWithMultipleRowGroups(partition, "data.parquet");

        var statisticsProvider = new FileSystemParquetStatisticsProvider(_extractor);
        var discovery = new FileSystemPartitionDiscovery(
            _testDirectory,
            statisticsProvider: statisticsProvider);

        // Act
        var partitions = discovery.DiscoverPartitions().ToList();

        // Assert
        var file = partitions[0].Files[0];
        Assert.That(file.RowGroups, Has.Count.EqualTo(2));
        
        var rg1 = file.RowGroups[0];
        var rg1Stats = rg1.ColumnStatisticsByPath["value"];
        rg1Stats.TryGetMinMax<int>(out var rg1Min, out var rg1Max);
        Assert.That(rg1Min, Is.EqualTo(1));
        Assert.That(rg1Max, Is.EqualTo(5));
        
        var rg2 = file.RowGroups[1];
        var rg2Stats = rg2.ColumnStatisticsByPath["value"];
        rg2Stats.TryGetMinMax<int>(out var rg2Min, out var rg2Max);
        Assert.That(rg2Min, Is.EqualTo(6));
        Assert.That(rg2Max, Is.EqualTo(10));
    }

    [Test]
    public void FileSystemDiscovery_WithCustomParallelism_EnrichesCorrectly()
    {
        // Arrange
        var partition = Path.Combine(_testDirectory, "year=2024");
        Directory.CreateDirectory(partition);
        ParquetTestFileHelper.CreateTestParquetFile(partition, "file1.parquet", new[] { 1, 2 });
        ParquetTestFileHelper.CreateTestParquetFile(partition, "file2.parquet", new[] { 3, 4 });
        ParquetTestFileHelper.CreateTestParquetFile(partition, "file3.parquet", new[] { 5, 6 });
        ParquetTestFileHelper.CreateTestParquetFile(partition, "file4.parquet", new[] { 7, 8 });

        var statisticsProvider = new FileSystemParquetStatisticsProvider(_extractor);
        var discovery = new FileSystemPartitionDiscovery(
            _testDirectory,
            statisticsProvider: statisticsProvider,
            statisticsParallelism: 2);

        // Act
        var partitions = discovery.DiscoverPartitions().ToList();

        // Assert
        Assert.That(partitions[0].Files, Has.Count.EqualTo(4));
        foreach (var file in partitions[0].Files)
        {
            Assert.That(file.RowCount, Is.EqualTo(2));
            Assert.That(file.RowGroups, Is.Not.Empty);
        }
    }

}

