using NSubstitute;

namespace ParquetSharpLINQ.Tests.Unit;

[TestFixture]
[Category("Unit")]
public class HiveParquetTableTests
{
    [SetUp]
    public void Setup()
    {
        _mockReader = Substitute.For<IParquetReader>();
        _testPath = "/test/data";
    }

    private IParquetReader _mockReader = null!;
    private string _testPath = null!;

    [Test]
    public void Constructor_WithNullPath_ThrowsArgumentNullException()
    {
        Assert.Throws<ArgumentNullException>(() => new HiveParquetTable<TestEntity>(null!));
    }

    [Test]
    public void Constructor_WithEntityWithoutAttributes_ThrowsInvalidOperationException()
    {
        var ex = Assert.Throws<InvalidOperationException>(() =>
            new HiveParquetTable<EntityWithoutAttributes>(_testPath, _mockReader));

        Assert.That(ex!.Message, Does.Contain("source-generated mapper"));
    }

    [Test]
    public void Constructor_WithValidEntity_Succeeds()
    {
        Assert.DoesNotThrow(() => new HiveParquetTable<TestEntity>(_testPath, _mockReader));
    }

    [Test]
    public void ElementType_ReturnsCorrectType()
    {
        var table = new HiveParquetTable<TestEntity>(_testPath, _mockReader);

        var elementType = table.ElementType;

        Assert.That(elementType, Is.EqualTo(typeof(TestEntity)));
    }

    [Test]
    public void Provider_ReturnsNonNullProvider()
    {
        var table = new HiveParquetTable<TestEntity>(_testPath, _mockReader);

        var provider = table.Provider;

        Assert.That(provider, Is.Not.Null);
    }

    [Test]
    public void AsQueryable_ReturnsSelf()
    {
        var table = new HiveParquetTable<TestEntity>(_testPath, _mockReader);

        var queryable = table.AsQueryable();

        Assert.That(queryable, Is.SameAs(table));
    }

    [Test]
    public void DiscoverPartitions_CallsPartitionDiscovery()
    {
        var tempDir = Path.Combine(Path.GetTempPath(), $"ParquetTest_{Guid.NewGuid()}");
        Directory.CreateDirectory(tempDir);
        try
        {
            var partition = Path.Combine(tempDir, "year=2024");
            Directory.CreateDirectory(partition);
            File.WriteAllText(Path.Combine(partition, "data.parquet"), "dummy");

            var table = new HiveParquetTable<TestEntity>(tempDir, _mockReader);

            var partitions = table.DiscoverPartitions().ToList();

            Assert.That(partitions, Has.Count.EqualTo(1));
            Assert.That(partitions[0].Values["year"], Is.EqualTo("2024"));
        }
        finally
        {
            if (Directory.Exists(tempDir)) Directory.Delete(tempDir, true);
        }
    }
}