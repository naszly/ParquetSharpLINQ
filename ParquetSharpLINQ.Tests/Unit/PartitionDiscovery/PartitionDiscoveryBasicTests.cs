using ParquetSharpLINQ.Discovery;

namespace ParquetSharpLINQ.Tests.Unit.PartitionDiscovery;

[TestFixture]
[Category("Unit")]
[Category("PartitionDiscovery")]
public class PartitionDiscoveryBasicTests
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
    public void Discover_WithNullPath_ThrowsArgumentException()
    {
        Assert.Throws<ArgumentException>(() => new FileSystemPartitionDiscovery(null!));
    }

    [Test]
    public void Discover_WithEmptyPath_ThrowsArgumentException()
    {
        Assert.Throws<ArgumentException>(() => new FileSystemPartitionDiscovery(""));
    }

    [Test]
    public void Discover_WithNonExistentDirectory_ThrowsDirectoryNotFoundException()
    {
        Assert.Throws<DirectoryNotFoundException>(() =>
            new FileSystemPartitionDiscovery("/nonexistent/path"));
    }

    [Test]
    public void Discover_WithNoParquetFiles_ReturnsEmpty()
    {
        var discovery = new FileSystemPartitionDiscovery(_testDirectory);
        var partitions = discovery.DiscoverPartitions().ToList();

        Assert.That(partitions, Is.Empty);
    }

    [Test]
    public void Discover_WithRootLevelParquetFile_ReturnsRootPartition()
    {
        var parquetFile = Path.Combine(_testDirectory, "data.parquet");
        File.WriteAllText(parquetFile, "dummy");

        var partitions = new FileSystemPartitionDiscovery(_testDirectory).DiscoverPartitions().ToList();

        Assert.That(partitions, Has.Count.EqualTo(1));
        Assert.That(partitions[0].Path, Is.EqualTo(_testDirectory));
        Assert.That(partitions[0].Values, Is.Empty);
    }

    [Test]
    public void Discover_WithMultipleFilesInPartition_ReturnsOnePartition()
    {
        var partition = Path.Combine(_testDirectory, "year=2024");
        Directory.CreateDirectory(partition);
        File.WriteAllText(Path.Combine(partition, "file1.parquet"), "dummy");
        File.WriteAllText(Path.Combine(partition, "file2.parquet"), "dummy");
        File.WriteAllText(Path.Combine(partition, "file3.parquet"), "dummy");

        var partitions = new FileSystemPartitionDiscovery(_testDirectory).DiscoverPartitions().ToList();

        Assert.That(partitions, Has.Count.EqualTo(1));
        Assert.That(partitions[0].Values["year"], Is.EqualTo("2024"));
    }

    [Test]
    public void Discover_IgnoresNonParquetFiles()
    {
        var dir = Path.Combine(_testDirectory, "year=2024");
        Directory.CreateDirectory(dir);
        File.WriteAllText(Path.Combine(dir, "file.txt"), "dummy");
        File.WriteAllText(Path.Combine(dir, "file.csv"), "dummy");

        var partitions = new FileSystemPartitionDiscovery(_testDirectory).DiscoverPartitions().ToList();

        Assert.That(partitions, Is.Empty);
    }

    [Test]
    public void Discover_WithInvalidPartitionFormat_TreatsAsNonPartitioned()
    {
        var dir = Path.Combine(_testDirectory, "data", "backup");
        Directory.CreateDirectory(dir);
        File.WriteAllText(Path.Combine(dir, "data.parquet"), "dummy");

        var partitions = new FileSystemPartitionDiscovery(_testDirectory).DiscoverPartitions().ToList();

        Assert.That(partitions, Has.Count.EqualTo(1));
        Assert.That(partitions[0].Values, Is.Empty);
    }

    [Test]
    public void Discover_WithCaseInsensitiveParquetExtension_FindsFiles()
    {
        var dir = Path.Combine(_testDirectory, "year=2024");
        Directory.CreateDirectory(dir);
        File.WriteAllText(Path.Combine(dir, "file.PARQUET"), "dummy");
        File.WriteAllText(Path.Combine(dir, "file.Parquet"), "dummy");

        var partitions = new FileSystemPartitionDiscovery(_testDirectory).DiscoverPartitions().ToList();

        Assert.That(partitions, Has.Count.EqualTo(1));
    }

    [Test]
    public void Discover_WithMixedPartitionedAndNonPartitionedDirectories_HandlesCorrectly()
    {
        var rootFile = Path.Combine(_testDirectory, "root.parquet");
        File.WriteAllText(rootFile, "dummy");

        var partitioned = Path.Combine(_testDirectory, "year=2024");
        Directory.CreateDirectory(partitioned);
        File.WriteAllText(Path.Combine(partitioned, "data.parquet"), "dummy");

        var nonPartitioned = Path.Combine(_testDirectory, "archive");
        Directory.CreateDirectory(nonPartitioned);
        File.WriteAllText(Path.Combine(nonPartitioned, "old.parquet"), "dummy");

        var partitions = new FileSystemPartitionDiscovery(_testDirectory).DiscoverPartitions().ToList();

        Assert.That(partitions, Has.Count.EqualTo(3));
        Assert.That(partitions.Any(p => p.Values.Count == 0 && p.Path == _testDirectory), Is.True);
        Assert.That(partitions.Any(p => p.Values.ContainsKey("year")), Is.True);
        Assert.That(partitions.Any(p => p.Path.EndsWith("archive")), Is.True);
    }
}

