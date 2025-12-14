using NSubstitute;

namespace ParquetSharpLINQ.Tests.Unit.DataReading;

[TestFixture]
[Category("Unit")]
[Category("DataReading")]
public class ParquetTableTests
{
    [SetUp]
    public void Setup()
    {
        _mockReader = Substitute.For<IParquetReader>();
        _mockDiscoveryStrategy = Substitute.For<IPartitionDiscoveryStrategy>();
    }

    private IParquetReader _mockReader = null!;
    private IPartitionDiscoveryStrategy _mockDiscoveryStrategy = null!;

    [Test]
    public void Constructor_WithNullPath_ThrowsArgumentException()
    {
        Assert.Throws<ArgumentException>(() => ParquetTable<TestEntity>.Factory.FromFileSystem(null!));
    }

    [Test]
    public void Constructor_WithEntityWithoutAttributes_ThrowsInvalidOperationException()
    {
        var ex = Assert.Throws<InvalidOperationException>(() =>
            new ParquetTable<EntityWithoutAttributes>(_mockDiscoveryStrategy, _mockReader));

        Assert.That(ex!.Message, Does.Contain("source-generated mapper"));
    }

    [Test]
    public void Constructor_WithValidEntity_Succeeds()
    {
        Assert.DoesNotThrow(() => new ParquetTable<TestEntity>(_mockDiscoveryStrategy, _mockReader));
    }

    [Test]
    public void ElementType_ReturnsCorrectType()
    {
        var table = new ParquetTable<TestEntity>(_mockDiscoveryStrategy, _mockReader);

        var elementType = table.ElementType;

        Assert.That(elementType, Is.EqualTo(typeof(TestEntity)));
    }

    [Test]
    public void Provider_ReturnsNonNullProvider()
    {
        var table = new ParquetTable<TestEntity>(_mockDiscoveryStrategy, _mockReader);

        var provider = table.Provider;

        Assert.That(provider, Is.Not.Null);
    }

    [Test]
    public void AsQueryable_ReturnsSelf()
    {
        var table = new ParquetTable<TestEntity>(_mockDiscoveryStrategy, _mockReader);

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

            var table = new ParquetTable<TestEntity>(new FileSystemPartitionDiscovery(tempDir), _mockReader);

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