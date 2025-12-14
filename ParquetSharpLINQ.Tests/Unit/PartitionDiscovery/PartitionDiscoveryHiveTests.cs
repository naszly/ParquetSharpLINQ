using ParquetSharpLINQ.Discovery;

namespace ParquetSharpLINQ.Tests.Unit.PartitionDiscovery;

[TestFixture]
[Category("Unit")]
[Category("PartitionDiscovery")]
public class PartitionDiscoveryHiveTests
{
    private string _testDirectory = null!;

    [SetUp]
    public void Setup()
    {
        _testDirectory = Path.Combine(Path.GetTempPath(), $"ParquetTest_{Guid.NewGuid()}");
        Directory.CreateDirectory(_testDirectory);
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
    public void Discover_WithHivePartitions_ParsesPartitionValues()
    {
        var partition1 = Path.Combine(_testDirectory, "year=2023", "region=US");
        Directory.CreateDirectory(partition1);
        File.WriteAllText(Path.Combine(partition1, "data.parquet"), "dummy");

        var partition2 = Path.Combine(_testDirectory, "year=2024", "region=EU");
        Directory.CreateDirectory(partition2);
        File.WriteAllText(Path.Combine(partition2, "data.parquet"), "dummy");

        var partitions = new FileSystemPartitionDiscovery(_testDirectory).DiscoverPartitions().ToList();

        Assert.That(partitions, Has.Count.EqualTo(2));

        var usPartition = partitions.First(p => p.Values.Values.Contains("US"));
        Assert.That(usPartition.Values["year"], Is.EqualTo("2023"));
        Assert.That(usPartition.Values["region"], Is.EqualTo("US"));

        var euPartition = partitions.First(p => p.Values.Values.Contains("EU"));
        Assert.That(euPartition.Values["year"], Is.EqualTo("2024"));
        Assert.That(euPartition.Values["region"], Is.EqualTo("EU"));
    }

    [Test]
    public void Discover_WithNestedPartitions_ParsesAllLevels()
    {
        var partition = Path.Combine(_testDirectory, "year=2024", "month=01", "day=15");
        Directory.CreateDirectory(partition);
        File.WriteAllText(Path.Combine(partition, "data.parquet"), "dummy");

        var partitions = new FileSystemPartitionDiscovery(_testDirectory).DiscoverPartitions().ToList();

        Assert.That(partitions, Has.Count.EqualTo(1));
        Assert.That(partitions[0].Values["year"], Is.EqualTo("2024"));
        Assert.That(partitions[0].Values["month"], Is.EqualTo("01"));
        Assert.That(partitions[0].Values["day"], Is.EqualTo("15"));
    }
}

