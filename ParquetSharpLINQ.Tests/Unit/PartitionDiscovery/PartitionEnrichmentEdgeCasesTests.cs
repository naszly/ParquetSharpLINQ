using ParquetSharpLINQ.Models;

namespace ParquetSharpLINQ.Tests.Unit.PartitionDiscovery;

[TestFixture]
[Category("Unit")]
[Category("PartitionDiscovery")]
public class PartitionEnrichmentEdgeCasesTests
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
    public void PartitionStatisticsEnricher_WithEmptyPartitions_ReturnsEmpty()
    {
        // Arrange
        var enricher = new PartitionStatisticsEnricher(
            new FileSystemParquetStatisticsProvider(_extractor));

        var emptyPartitions = Enumerable.Empty<Partition>();

        // Act
        var result = enricher.Enrich(emptyPartitions).ToList();

        // Assert
        Assert.That(result, Is.Empty);
    }

    [Test]
    public void PartitionStatisticsEnricher_WithNoFiles_ReturnsOriginalPartitions()
    {
        // Arrange
        var enricher = new PartitionStatisticsEnricher(
            new FileSystemParquetStatisticsProvider(_extractor));

        var partitionsWithNoFiles = new[]
        {
            new Partition
            {
                Path = "/some/path",
                Values = new Dictionary<string, string> { ["year"] = "2024" },
                Files = Array.Empty<ParquetFile>()
            }
        };

        // Act
        var result = enricher.Enrich(partitionsWithNoFiles).ToList();

        // Assert
        Assert.That(result, Has.Count.EqualTo(1));
        Assert.That(result[0].Path, Is.EqualTo("/some/path"));
        Assert.That(result[0].Files, Is.Empty);
    }

    [Test]
    public void PartitionStatisticsEnricher_PreservesPartitionValues()
    {
        // Arrange
        var partition = Path.Combine(_testDirectory, "year=2024", "month=12");
        Directory.CreateDirectory(partition);
        ParquetTestFileHelper.CreateTestParquetFile(partition, "data.parquet", new[] { 1, 2, 3 });

        var statisticsProvider = new FileSystemParquetStatisticsProvider(_extractor);
        var discovery = new FileSystemPartitionDiscovery(
            _testDirectory,
            statisticsProvider: statisticsProvider);

        // Act
        var partitions = discovery.DiscoverPartitions().ToList();

        // Assert
        Assert.That(partitions[0].Values["year"], Is.EqualTo("2024"));
        Assert.That(partitions[0].Values["month"], Is.EqualTo("12"));
        Assert.That(partitions[0].Files[0].RowCount, Is.EqualTo(3));
    }

    [Test]
    public void PartitionStatisticsEnricher_WithDifferentFileTypes_EnrichesAll()
    {
        // Arrange
        var partition = Path.Combine(_testDirectory, "data");
        Directory.CreateDirectory(partition);
        
        ParquetTestFileHelper.CreateTestParquetFileWithIntegers(partition, "ints.parquet", new[] { 1, 2, 3 });
        ParquetTestFileHelper.CreateTestParquetFileWithLongs(partition, "longs.parquet", new[] { 100L, 200L, 300L });
        ParquetTestFileHelper.CreateTestParquetFileWithStrings(partition, "strings.parquet", new[] { "a", "b", "c" });

        var statisticsProvider = new FileSystemParquetStatisticsProvider(_extractor);
        var discovery = new FileSystemPartitionDiscovery(
            _testDirectory,
            statisticsProvider: statisticsProvider);

        // Act
        var partitions = discovery.DiscoverPartitions().ToList();

        // Assert
        Assert.That(partitions[0].Files, Has.Count.EqualTo(3));
        
        foreach (var file in partitions[0].Files)
        {
            Assert.That(file.RowCount, Is.EqualTo(3));
            Assert.That(file.RowGroups, Is.Not.Empty);
            Assert.That(file.RowGroups[0].ColumnStatisticsByPath, Is.Not.Empty);
        }
    }
}

