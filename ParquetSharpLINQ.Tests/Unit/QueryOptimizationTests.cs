using NSubstitute;
using ParquetSharp;
using ParquetSharpLINQ.ParquetSharp;

namespace ParquetSharpLINQ.Tests.Unit;

[TestFixture]
[Category("Unit")]
public class QueryOptimizationTests
{
    [SetUp]
    public void Setup()
    {
        _mockReader = Substitute.For<IParquetReader>();

        _testPath = Path.Combine(Path.GetTempPath(), $"ParquetOptTest_{Guid.NewGuid()}");
        Directory.CreateDirectory(_testPath);

        CreatePartitionedStructure();
        SetupMockReader();
    }

    [TearDown]
    public void TearDown()
    {
        if (Directory.Exists(_testPath)) Directory.Delete(_testPath, true);
    }

    private IParquetReader _mockReader = null!;
    private string _testPath = null!;

    private void CreatePartitionedStructure()
    {
        // year=2023/region=us
        var partition1 = Path.Combine(_testPath, "year=2023", "region=us");
        Directory.CreateDirectory(partition1);
        File.WriteAllText(Path.Combine(partition1, "data.parquet"), "dummy");

        // year=2023/region=eu
        var partition2 = Path.Combine(_testPath, "year=2023", "region=eu");
        Directory.CreateDirectory(partition2);
        File.WriteAllText(Path.Combine(partition2, "data.parquet"), "dummy");

        // year=2024/region=us
        var partition3 = Path.Combine(_testPath, "year=2024", "region=us");
        Directory.CreateDirectory(partition3);
        File.WriteAllText(Path.Combine(partition3, "data.parquet"), "dummy");

        // year=2024/region=eu
        var partition4 = Path.Combine(_testPath, "year=2024", "region=eu");
        Directory.CreateDirectory(partition4);
        File.WriteAllText(Path.Combine(partition4, "data.parquet"), "dummy");
    }

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

        _mockReader.ReadRows(Arg.Any<string>(), Arg.Any<IEnumerable<string>>()).Returns(callInfo =>
        {
            var filePath = callInfo.Arg<string>();

            if (filePath.Contains(Path.DirectorySeparatorChar + "year=2023") &&
                filePath.Contains(Path.DirectorySeparatorChar + "region=us"))
                return new List<ParquetRow>
                {
                    new(["id", "name", "amount", "count", "is_active", "created_date"],
                        [1L, "Alice_2023_us", 100m, 10, true, new DateTime(2023, 1, 1)])
                };

            if (filePath.Contains(Path.DirectorySeparatorChar + "year=2023") &&
                filePath.Contains(Path.DirectorySeparatorChar + "region=eu"))
                return new List<ParquetRow>
                {
                    new(["id", "name", "amount", "count", "is_active", "created_date"],
                        [2L, "Bob_2023_eu", 200m, 20, true, new DateTime(2023, 1, 1)])
                };

            if (filePath.Contains(Path.DirectorySeparatorChar + "year=2024") &&
                filePath.Contains(Path.DirectorySeparatorChar + "region=us"))
                return new List<ParquetRow>
                {
                    new(["id", "name", "amount", "count", "is_active", "created_date"],
                        [3L, "Charlie_2024_us", 300m, 30, true, new DateTime(2024, 1, 1)])
                };

            if (filePath.Contains(Path.DirectorySeparatorChar + "year=2024") &&
                filePath.Contains(Path.DirectorySeparatorChar + "region=eu"))
                return new List<ParquetRow>
                {
                    new(["id", "name", "amount", "count", "is_active", "created_date"],
                        [4L, "David_2024_eu", 400m, 40, true, new DateTime(2024, 1, 1)])
                };

            return new List<ParquetRow>();
        });
    }

    [Test]
    public void PartitionPruning_WithYearFilter_OnlyReadsMatchingPartitions()
    {
        var table = new ParquetTable<TestEntity>(_testPath, _mockReader);
        var callCount = 0;

        _mockReader.When(x => x.ReadRows(Arg.Any<string>(), Arg.Any<IEnumerable<string>>()))
            .Do(_ => callCount++);

        var results = table.Where(e => e.Year == 2024).ToList();

        Assert.That(results, Has.Count.EqualTo(2));
        Assert.That(results.All(r => r.Year == 2024), Is.True);
        Assert.That(callCount, Is.EqualTo(2));
    }

    [Test]
    public void PartitionPruning_WithMultipleFilters_OnlyReadsMatchingPartition()
    {
        var table = new ParquetTable<TestEntity>(_testPath, _mockReader);
        var callCount = 0;

        _mockReader.When(x => x.ReadRows(Arg.Any<string>(), Arg.Any<IEnumerable<string>>()))
            .Do(_ => callCount++);

        var results = table.Where(e => e.Year == 2024 && e.Region == "us").ToList();

        Assert.That(results, Has.Count.EqualTo(1));
        Assert.That(results[0].Year, Is.EqualTo(2024));
        Assert.That(results[0].Region, Is.EqualTo("us"));
        Assert.That(callCount, Is.EqualTo(1));
    }

    [Test]
    public void PartitionPruning_WithNoFilters_ReadsAllPartitions()
    {
        var table = new ParquetTable<TestEntity>(_testPath, _mockReader);
        var callCount = 0;

        _mockReader.When(x => x.ReadRows(Arg.Any<string>(), Arg.Any<IEnumerable<string>>()))
            .Do(_ => callCount++);

        var results = table.Where(e => e.Amount > 0).ToList();

        Assert.That(results, Has.Count.EqualTo(4));
        Assert.That(callCount, Is.EqualTo(4));
    }

    [Test]
    public void ColumnProjection_WithSelectProjection_OnlyReadsRequestedColumns()
    {
        var table = new ParquetTable<TestEntity>(_testPath, _mockReader);
        IEnumerable<string>? requestedColumns = null;

        _mockReader.ReadRows(Arg.Any<string>(), Arg.Any<IEnumerable<string>>())
            .Returns(callInfo =>
            {
                requestedColumns = callInfo.Arg<IEnumerable<string>>();
                return new List<ParquetRow>
                {
                    new(["id", "name"], [1L, "Test"])
                };
            });

        var results = table
            .Where(e => e.Year == 2024)
            .Select(e => new { e.Id, e.Name })
            .ToList();

        Assert.That(requestedColumns, Is.Not.Null);
        var columnsList = requestedColumns!.ToList();

        Assert.That(columnsList, Does.Contain("id"));
        Assert.That(columnsList, Does.Contain("name"));
        Assert.That(columnsList, Does.Not.Contain("count"));
        Assert.That(columnsList, Does.Not.Contain("is_active"));
    }

    [Test]
    public void CombinedOptimization_PartitionPruningAndColumnProjection()
    {
        var table = new ParquetTable<TestEntity>(_testPath, _mockReader);
        var callCount = 0;
        IEnumerable<string>? requestedColumns = null;

        _mockReader.ReadRows(Arg.Any<string>(), Arg.Any<IEnumerable<string>>())
            .Returns(callInfo =>
            {
                callCount++;
                requestedColumns = callInfo.Arg<IEnumerable<string>>();
                return new List<ParquetRow>
                {
                    new(["id", "amount"], [1L, 100m])
                };
            });

        var results = table
            .Where(e => e.Year == 2024 && e.Region == "us")
            .Select(e => new { e.Id, e.Amount })
            .ToList();

        // Assert - Partition pruning
        Assert.That(callCount, Is.EqualTo(1));

        // Assert - Column projection
        Assert.That(requestedColumns, Is.Not.Null);
        var columnsList = requestedColumns!.ToList();
        Assert.That(columnsList, Has.Count.LessThanOrEqualTo(3),
            "Should read fewer columns than available");
    }

    [Test]
    public void PartitionValues_AreCorrectlyEnriched()
    {
        var table = new ParquetTable<TestEntity>(_testPath, _mockReader);

        var results = table.Where(e => e.Year == 2024 && e.Region == "eu").ToList();

        Assert.That(results, Has.Count.EqualTo(1));
        var result = results[0];
        Assert.That(result.Year, Is.EqualTo(2024));
        Assert.That(result.Region, Is.EqualTo("eu"));
        Assert.That(result.Name, Is.EqualTo("David_2024_eu"));
    }

    [Test]
    public void PartitionPruning_WithNumericFilters_MatchesLeadingZeroDirectories()
    {
        var testPath = Path.Combine(Path.GetTempPath(), $"NumericPartitionTest_{Guid.NewGuid()}");
        Directory.CreateDirectory(testPath);

        try
        {
            // Create partition with leading zeros in year: year=0024/region=us
            var partitionPath = Path.Combine(testPath, "year=0024", "region=us");
            Directory.CreateDirectory(partitionPath);
            File.WriteAllText(Path.Combine(partitionPath, "data.parquet"), "dummy");

            var mockReader = Substitute.For<IParquetReader>();

            var columns = new List<Column>
            {
                new(typeof(object), "id"),
                new(typeof(object), "name"),
                new(typeof(object), "amount"),
                new(typeof(object), "count"),
                new(typeof(object), "is_active"),
                new(typeof(object), "created_date")
            };
            mockReader.GetColumns(Arg.Any<string>()).Returns(columns);

            mockReader.ReadRows(Arg.Any<string>(), Arg.Any<IEnumerable<string>>()).Returns(
                new List<ParquetRow>
                {
                    new(["id", "name", "amount", "count", "is_active", "created_date"],
                        [1L, "Test", 100m, 10, true, new DateTime(2024, 1, 1)])
                });

            var table = new ParquetTable<TestEntity>(testPath, mockReader);
            var callCount = 0;

            mockReader.When(x => x.ReadRows(Arg.Any<string>(), Arg.Any<IEnumerable<string>>()))
                .Do(_ => callCount++);

            var results = table.Where(e => e.Year == 24 && e.Region == "us").ToList();

            Assert.That(results, Has.Count.EqualTo(1));
            Assert.That(callCount, Is.EqualTo(1));
            Assert.That(results[0].Year, Is.EqualTo(24));
            Assert.That(results[0].Region, Is.EqualTo("us"));
        }
        finally
        {
            if (Directory.Exists(testPath))
                Directory.Delete(testPath, true);
        }
    }

    [Test]
    public void PartitionPruning_WithDateTimeFilters_MatchesIsoFormattedDirectories()
    {
        var testPath = Path.Combine(Path.GetTempPath(), $"DateTimePartitionTest_{Guid.NewGuid()}");
        Directory.CreateDirectory(testPath);

        try
        {
            // Create partition with ISO date format: event_date=2024-12-07/region=us
            var partitionPath = Path.Combine(testPath, "event_date=2024-12-07", "region=us");
            Directory.CreateDirectory(partitionPath);
            File.WriteAllText(Path.Combine(partitionPath, "data.parquet"), "dummy");

            var mockReader = Substitute.For<IParquetReader>();

            var columns = new List<Column>
            {
                new(typeof(object), "id"),
                new(typeof(object), "name")
            };
            mockReader.GetColumns(Arg.Any<string>()).Returns(columns);

            mockReader.ReadRows(Arg.Any<string>(), Arg.Any<IEnumerable<string>>()).Returns(
                new List<ParquetRow>
                {
                    new(["id", "name"], [1L, "Test"])
                });

            var table = new ParquetTable<TestEntityWithDateTimePartition>(testPath, mockReader);
            var callCount = 0;

            mockReader.When(x => x.ReadRows(Arg.Any<string>(), Arg.Any<IEnumerable<string>>()))
                .Do(_ => callCount++);

            var filterDate = new DateTime(2024, 12, 7);
            var results = table.Where(e => e.EventDate == filterDate && e.Region == "us").ToList();

            Assert.That(results, Has.Count.EqualTo(1),
                "Should find record in event_date=2024-12-07 partition when filtering with DateTime(2024, 12, 7)");
            Assert.That(callCount, Is.EqualTo(1));
        }
        finally
        {
            if (Directory.Exists(testPath))
                Directory.Delete(testPath, true);
        }
    }

    [Test]
    public void PartitionPruning_WithDateOnlyFilters_MatchesIsoFormattedDirectories()
    {
        var testPath = Path.Combine(Path.GetTempPath(), $"DateOnlyPartitionTest_{Guid.NewGuid()}");
        Directory.CreateDirectory(testPath);

        try
        {
            // Create partition with ISO date format: data_day=2024-12-07/region=us
            var partitionPath = Path.Combine(testPath, "data_day=2024-12-07", "region=us");
            Directory.CreateDirectory(partitionPath);
            File.WriteAllText(Path.Combine(partitionPath, "data.parquet"), "dummy");

            var mockReader = Substitute.For<IParquetReader>();

            var columns = new List<Column>
            {
                new(typeof(object), "id"),
                new(typeof(object), "name")
            };
            mockReader.GetColumns(Arg.Any<string>()).Returns(columns);

            mockReader.ReadRows(Arg.Any<string>(), Arg.Any<IEnumerable<string>>()).Returns(
                new List<ParquetRow>
                {
                    new(["id", "name"], [1L, "Test"])
                });

            var table = new ParquetTable<TestEntityWithDateOnlyPartition>(testPath, mockReader);
            var callCount = 0;

            mockReader.When(x => x.ReadRows(Arg.Any<string>(), Arg.Any<IEnumerable<string>>()))
                .Do(_ => callCount++);

            var filterDate = new DateOnly(2024, 12, 7);
            var results = table.Where(e => e.DataDay == filterDate && e.Region == "us").ToList();

            Assert.That(results, Has.Count.EqualTo(1),
                "Should find record in data_day=2024-12-07 partition when filtering with DateOnly(2024, 12, 7)");
            Assert.That(callCount, Is.EqualTo(1));
        }
        finally
        {
            if (Directory.Exists(testPath))
                Directory.Delete(testPath, true);
        }
    }
}