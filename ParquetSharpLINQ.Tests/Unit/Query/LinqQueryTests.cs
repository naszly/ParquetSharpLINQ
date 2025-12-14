using NSubstitute;
using ParquetSharp;
using ParquetSharpLINQ.Discovery;
using ParquetSharpLINQ.Interfaces;
using ParquetSharpLINQ.ParquetSharp;

namespace ParquetSharpLINQ.Tests.Unit.Query;

[TestFixture]
[Category("Unit")]
[Category("Query")]
public class LinqQueryTests
{
    [SetUp]
    public void Setup()
    {
        _mockReader = Substitute.For<IParquetReader>();

        _testPath = Path.Combine(Path.GetTempPath(), $"ParquetLinqTest_{Guid.NewGuid()}");
        Directory.CreateDirectory(_testPath);

        var parquetFile = Path.Combine(_testPath, "data.parquet");
        File.WriteAllText(parquetFile, "dummy");

        SetupMockReader();
    }

    [TearDown]
    public void TearDown()
    {
        if (Directory.Exists(_testPath)) Directory.Delete(_testPath, true);
    }

    private IParquetReader _mockReader = null!;
    private string _testPath = null!;

    private void SetupMockReader()
    {
        var columns = new List<Column>
        {
            new(typeof(object), "id"),
            new(typeof(object), "name"),
            new(typeof(object), "amount"),
            new(typeof(object), "count"),
            new(typeof(object), "is_active"),
            new(typeof(object), "created_date")
        };
        _mockReader.GetColumns(Arg.Any<string>()).Returns(columns);

        var rows = new List<ParquetRow>
        {
            new(["id", "name", "amount", "count", "is_active", "created_date"],
                [1L, "Alice", 100.50m, 5, true, new DateTime(2024, 1, 1)]),
            new(["id", "name", "amount", "count", "is_active", "created_date"],
                [2L, "Bob", 250.75m, 10, true, new DateTime(2024, 2, 1)]),
            new(["id", "name", "amount", "count", "is_active", "created_date"],
                [3L, "Charlie", 75.25m, 3, false, new DateTime(2024, 3, 1)])
        };
        _mockReader.ReadRows(Arg.Any<string>(), Arg.Any<IEnumerable<string>>()).Returns(rows);
    }

    [Test]
    public void Where_FiltersByPredicate()
    {
        var table = new ParquetTable<TestEntity>(new FileSystemPartitionDiscovery(_testPath), _mockReader);

        var results = table.Where(e => e.Amount > 200).ToList();

        Assert.That(results, Has.Count.EqualTo(1));
        Assert.That(results[0].Name, Is.EqualTo("Bob"));
    }

    [Test]
    public void Where_WithMultipleConditions_FiltersCorrectly()
    {
        var table = new ParquetTable<TestEntity>(new FileSystemPartitionDiscovery(_testPath), _mockReader);

        var results = table
            .Where(e => e.Amount > 50)
            .Where(e => e.IsActive)
            .ToList();

        Assert.That(results, Has.Count.EqualTo(2));
        Assert.That(results.All(r => r.IsActive), Is.True);
    }

    [Test]
    public void Select_ProjectsProperties()
    {
        var table = new ParquetTable<TestEntity>(new FileSystemPartitionDiscovery(_testPath), _mockReader);

        var results = table.Select(e => new { e.Name, e.Amount }).ToList();

        Assert.That(results, Has.Count.EqualTo(3));
        Assert.That(results[0].Name, Is.EqualTo("Alice"));
        Assert.That(results[0].Amount, Is.EqualTo(100.50m));
    }

    [Test]
    public void OrderBy_SortsAscending()
    {
        var table = new ParquetTable<TestEntity>(new FileSystemPartitionDiscovery(_testPath), _mockReader);

        var results = table.OrderBy(e => e.Amount).ToList();

        Assert.That(results, Has.Count.EqualTo(3));
        Assert.That(results[0].Name, Is.EqualTo("Charlie"));
        Assert.That(results[1].Name, Is.EqualTo("Alice"));
        Assert.That(results[2].Name, Is.EqualTo("Bob"));
    }

    [Test]
    public void OrderByDescending_SortsDescending()
    {
        var table = new ParquetTable<TestEntity>(new FileSystemPartitionDiscovery(_testPath), _mockReader);

        var results = table.OrderByDescending(e => e.Amount).ToList();

        Assert.That(results, Has.Count.EqualTo(3));
        Assert.That(results[0].Name, Is.EqualTo("Bob"));
        Assert.That(results[1].Name, Is.EqualTo("Alice"));
        Assert.That(results[2].Name, Is.EqualTo("Charlie"));
    }

    [Test]
    public void Take_LimitsResults()
    {
        var table = new ParquetTable<TestEntity>(new FileSystemPartitionDiscovery(_testPath), _mockReader);

        var results = table.Take(2).ToList();

        Assert.That(results, Has.Count.EqualTo(2));
    }

    [Test]
    public void Skip_SkipsRecords()
    {
        var table = new ParquetTable<TestEntity>(new FileSystemPartitionDiscovery(_testPath), _mockReader);

        var results = table.Skip(1).ToList();

        Assert.That(results, Has.Count.EqualTo(2));
        Assert.That(results[0].Name, Is.EqualTo("Bob"));
    }

    [Test]
    public void First_ReturnsFirstElement()
    {
        var table = new ParquetTable<TestEntity>(new FileSystemPartitionDiscovery(_testPath), _mockReader);

        var result = table.First();

        Assert.That(result.Name, Is.EqualTo("Alice"));
    }

    [Test]
    public void FirstOrDefault_WithNoMatch_ReturnsNull()
    {
        var table = new ParquetTable<TestEntity>(new FileSystemPartitionDiscovery(_testPath), _mockReader);

        var result = table.FirstOrDefault(e => e.Amount > 1000);

        Assert.That(result, Is.Null);
    }

    [Test]
    public void Count_ReturnsCorrectCount()
    {
        var table = new ParquetTable<TestEntity>(new FileSystemPartitionDiscovery(_testPath), _mockReader);

        var count = table.Count();

        Assert.That(count, Is.EqualTo(3));
    }

    [Test]
    public void Count_WithPredicate_ReturnsFilteredCount()
    {
        var table = new ParquetTable<TestEntity>(new FileSystemPartitionDiscovery(_testPath), _mockReader);

        var count = table.Count(e => e.IsActive);

        Assert.That(count, Is.EqualTo(2));
    }

    [Test]
    public void Any_WithMatchingPredicate_ReturnsTrue()
    {
        var table = new ParquetTable<TestEntity>(new FileSystemPartitionDiscovery(_testPath), _mockReader);

        var result = table.Any(e => e.Name == "Bob");

        Assert.That(result, Is.True);
    }

    [Test]
    public void Any_WithoutMatch_ReturnsFalse()
    {
        var table = new ParquetTable<TestEntity>(new FileSystemPartitionDiscovery(_testPath), _mockReader);

        var result = table.Any(e => e.Name == "Nonexistent");

        Assert.That(result, Is.False);
    }

    [Test]
    public void GroupBy_GroupsCorrectly()
    {
        var table = new ParquetTable<TestEntity>(new FileSystemPartitionDiscovery(_testPath), _mockReader);

        var results = table
            .GroupBy(e => e.IsActive)
            .Select(g => new { IsActive = g.Key, Count = g.Count() })
            .ToList();

        Assert.That(results, Has.Count.EqualTo(2));
        Assert.That(results.First(r => r.IsActive).Count, Is.EqualTo(2));
        Assert.That(results.First(r => !r.IsActive).Count, Is.EqualTo(1));
    }

    [Test]
    public void Sum_CalculatesCorrectly()
    {
        var table = new ParquetTable<TestEntity>(new FileSystemPartitionDiscovery(_testPath), _mockReader);

        var sum = table.Sum(e => e.Amount);

        Assert.That(sum, Is.EqualTo(426.50m));
    }

    [Test]
    public void Average_CalculatesCorrectly()
    {
        var table = new ParquetTable<TestEntity>(new FileSystemPartitionDiscovery(_testPath), _mockReader);

        var average = table.Average(e => e.Count);

        Assert.That(average, Is.EqualTo(6.0));
    }

    [Test]
    public void Min_ReturnsMinimumValue()
    {
        var table = new ParquetTable<TestEntity>(new FileSystemPartitionDiscovery(_testPath), _mockReader);

        var min = table.Min(e => e.Amount);

        Assert.That(min, Is.EqualTo(75.25m));
    }

    [Test]
    public void Max_ReturnsMaximumValue()
    {
        var table = new ParquetTable<TestEntity>(new FileSystemPartitionDiscovery(_testPath), _mockReader);

        var max = table.Max(e => e.Amount);

        Assert.That(max, Is.EqualTo(250.75m));
    }

    [Test]
    public void ComplexQuery_CombinesMultipleOperations()
    {
        var table = new ParquetTable<TestEntity>(new FileSystemPartitionDiscovery(_testPath), _mockReader);

        var results = table
            .Where(e => e.IsActive)
            .OrderByDescending(e => e.Amount)
            .Select(e => new { e.Name, e.Amount })
            .Take(1)
            .ToList();

        Assert.That(results, Has.Count.EqualTo(1));
        Assert.That(results[0].Name, Is.EqualTo("Bob"));
        Assert.That(results[0].Amount, Is.EqualTo(250.75m));
    }
}