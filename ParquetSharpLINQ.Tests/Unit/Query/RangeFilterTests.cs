using ParquetSharp;
using ParquetSharpLINQ.Attributes;
using ParquetSharpLINQ.Query;

namespace ParquetSharpLINQ.Tests.Unit.Query;

// Test entity classes at namespace level for source generator
public class RangeTestEntity
{
    [ParquetColumn("id")]
    public long Id { get; set; }

    [ParquetColumn("value")]
    public int Value { get; set; }
}

public class RangeTestEntityWithDouble
{
    [ParquetColumn("id")]
    public long Id { get; set; }

    [ParquetColumn("price")]
    public double Price { get; set; }
}

public class RangeTestEntityWithDate
{
    [ParquetColumn("id")]
    public long Id { get; set; }

    [ParquetColumn("event_date")]
    public DateTime EventDate { get; set; }
}

public class RangeTestEntityWithDateOnly
{
    [ParquetColumn("id")]
    public long Id { get; set; }

    [ParquetColumn("event_date")]
    public DateOnly EventDate { get; set; }
}

public class RangeTestEntityWithString
{
    [ParquetColumn("id")]
    public long Id { get; set; }

    [ParquetColumn("name")]
    public string Name { get; set; } = string.Empty;
}

public class RangeTestEntityWithBool
{
    [ParquetColumn("id")]
    public long Id { get; set; }

    [ParquetColumn("is_active")]
    public bool IsActive { get; set; }
}

[TestFixture]
[Category("Unit")]
public class RangeFilterTests
{
    private string _testDirectory = null!;

    [SetUp]
    public void Setup()
    {
        _testDirectory = Path.Combine(Path.GetTempPath(), $"RangeFilterTest_{Guid.NewGuid()}");
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

    #region Greater Than Tests

    [Test]
    public void RangeFilter_GreaterThan_ExtractsMinConstraint()
    {
        // Arrange
        CreateTestData();
        using var table = ParquetTable<RangeTestEntity>.Factory.FromFileSystem(_testDirectory);

        // Act - Real LINQ expression: x.Value > 50
        var query = table.Where(x => x.Value > 50);
        var expression = query.Expression;
        var analysis = QueryAnalyzer.Analyze(expression);

        // Assert
        Assert.That(analysis.RangeFilters, Contains.Key("Value"));
        var filter = analysis.RangeFilters["Value"];
        Assert.That(filter.Min, Is.EqualTo(50));
        Assert.That(filter.MinInclusive, Is.False, "Greater than should be exclusive");
        Assert.That(filter.Max, Is.Null, "No upper bound");
    }

    [Test]
    public void RangeFilter_GreaterThanOrEqual_ExtractsMinConstraint()
    {
        // Arrange
        CreateTestData();
        using var table = ParquetTable<RangeTestEntity>.Factory.FromFileSystem(_testDirectory);

        // Act - Real LINQ expression: x.Value >= 50
        var query = table.Where(x => x.Value >= 50);
        var expression = query.Expression;
        var analysis = QueryAnalyzer.Analyze(expression);

        // Assert
        Assert.That(analysis.RangeFilters, Contains.Key("Value"));
        var filter = analysis.RangeFilters["Value"];
        Assert.That(filter.Min, Is.EqualTo(50));
        Assert.That(filter.MinInclusive, Is.True, "Greater than or equal should be inclusive");
        Assert.That(filter.Max, Is.Null);
    }

    #endregion

    #region Less Than Tests

    [Test]
    public void RangeFilter_LessThan_ExtractsMaxConstraint()
    {
        // Arrange
        CreateTestData();
        using var table = ParquetTable<RangeTestEntity>.Factory.FromFileSystem(_testDirectory);

        // Act - Real LINQ expression: x.Value < 100
        var query = table.Where(x => x.Value < 100);
        var expression = query.Expression;
        var analysis = QueryAnalyzer.Analyze(expression);

        // Assert
        Assert.That(analysis.RangeFilters, Contains.Key("Value"));
        var filter = analysis.RangeFilters["Value"];
        Assert.That(filter.Min, Is.Null, "No lower bound");
        Assert.That(filter.Max, Is.EqualTo(100));
        Assert.That(filter.MaxInclusive, Is.False, "Less than should be exclusive");
    }

    [Test]
    public void RangeFilter_LessThanOrEqual_ExtractsMaxConstraint()
    {
        // Arrange
        CreateTestData();
        using var table = ParquetTable<RangeTestEntity>.Factory.FromFileSystem(_testDirectory);

        // Act - Real LINQ expression: x.Value <= 100
        var query = table.Where(x => x.Value <= 100);
        var expression = query.Expression;
        var analysis = QueryAnalyzer.Analyze(expression);

        // Assert
        Assert.That(analysis.RangeFilters, Contains.Key("Value"));
        var filter = analysis.RangeFilters["Value"];
        Assert.That(filter.Min, Is.Null);
        Assert.That(filter.Max, Is.EqualTo(100));
        Assert.That(filter.MaxInclusive, Is.True, "Less than or equal should be inclusive");
    }

    #endregion

    #region Equal Tests

    [Test]
    public void RangeFilter_Equal_ExtractsMinAndMaxConstraint()
    {
        // Arrange
        CreateTestData();
        using var table = ParquetTable<RangeTestEntity>.Factory.FromFileSystem(_testDirectory);

        // Act - Real LINQ expression: x.Value == 75
        var query = table.Where(x => x.Value == 75);
        var expression = query.Expression;
        var analysis = QueryAnalyzer.Analyze(expression);

        // Assert
        Assert.That(analysis.RangeFilters, Contains.Key("Value"));
        var filter = analysis.RangeFilters["Value"];
        Assert.That(filter.Min, Is.EqualTo(75));
        Assert.That(filter.Max, Is.EqualTo(75));
        Assert.That(filter.MinInclusive, Is.True, "Equal should be inclusive");
        Assert.That(filter.MaxInclusive, Is.True, "Equal should be inclusive");
    }

    #endregion

    #region Combined Range Tests

    [Test]
    public void RangeFilter_CombinedRange_ExtractsMinAndMax()
    {
        // Arrange
        CreateTestData();
        using var table = ParquetTable<RangeTestEntity>.Factory.FromFileSystem(_testDirectory);

        // Act - Real LINQ expression: x.Value >= 50 && x.Value < 100
        var query = table.Where(x => x.Value >= 50 && x.Value < 100);
        var expression = query.Expression;
        var analysis = QueryAnalyzer.Analyze(expression);

        // Assert
        Assert.That(analysis.RangeFilters, Contains.Key("Value"));
        var filter = analysis.RangeFilters["Value"];
        Assert.That(filter.Min, Is.EqualTo(50));
        Assert.That(filter.MinInclusive, Is.True);
        Assert.That(filter.Max, Is.EqualTo(100));
        Assert.That(filter.MaxInclusive, Is.False);
    }

    [Test]
    public void RangeFilter_CombinedRangeReversed_ExtractsMinAndMax()
    {
        // Arrange
        CreateTestData();
        using var table = ParquetTable<RangeTestEntity>.Factory.FromFileSystem(_testDirectory);

        // Act - Real LINQ expression: x.Value < 100 && x.Value >= 50 (reversed order)
        var query = table.Where(x => x.Value < 100 && x.Value >= 50);
        var expression = query.Expression;
        var analysis = QueryAnalyzer.Analyze(expression);

        // Assert
        Assert.That(analysis.RangeFilters, Contains.Key("Value"));
        var filter = analysis.RangeFilters["Value"];
        Assert.That(filter.Min, Is.EqualTo(50));
        Assert.That(filter.Max, Is.EqualTo(100));
    }

    [Test]
    public void RangeFilter_MultipleColumns_ExtractsAllFilters()
    {
        // Arrange
        CreateTestData();
        using var table = ParquetTable<RangeTestEntity>.Factory.FromFileSystem(_testDirectory);

        // Act - Real LINQ expression: multiple columns
        var query = table.Where(x => x.Value >= 50 && x.Id < 1000);
        var expression = query.Expression;
        var analysis = QueryAnalyzer.Analyze(expression);

        // Assert
        Assert.That(analysis.RangeFilters, Contains.Key("Value"));
        Assert.That(analysis.RangeFilters, Contains.Key("Id"));

        var valueFilter = analysis.RangeFilters["Value"];
        Assert.That(valueFilter.Min, Is.EqualTo(50));
        Assert.That(valueFilter.Max, Is.Null);

        var idFilter = analysis.RangeFilters["Id"];
        Assert.That(idFilter.Min, Is.Null);
        Assert.That(idFilter.Max, Is.EqualTo(1000));
        Assert.That(idFilter.MaxInclusive, Is.False);
    }

    #endregion

    #region Reversed Operand Tests

    [Test]
    public void RangeFilter_ReversedOperands_GreaterThan_ExtractsCorrectly()
    {
        // Arrange
        CreateTestData();
        using var table = ParquetTable<RangeTestEntity>.Factory.FromFileSystem(_testDirectory);

        // Act - Real LINQ expression: 50 < x.Value (reversed)
        var query = table.Where(x => 50 < x.Value);
        var expression = query.Expression;
        var analysis = QueryAnalyzer.Analyze(expression);

        // Assert - 50 < x.Value means x.Value > 50
        Assert.That(analysis.RangeFilters, Contains.Key("Value"));
        var filter = analysis.RangeFilters["Value"];
        Assert.That(filter.Min, Is.EqualTo(50));
        Assert.That(filter.MinInclusive, Is.False);
    }

    [Test]
    public void RangeFilter_ReversedOperands_LessThan_ExtractsCorrectly()
    {
        // Arrange
        CreateTestData();
        using var table = ParquetTable<RangeTestEntity>.Factory.FromFileSystem(_testDirectory);

        // Act - Real LINQ expression: 100 > x.Value (reversed)
        var query = table.Where(x => 100 > x.Value);
        var expression = query.Expression;
        var analysis = QueryAnalyzer.Analyze(expression);

        // Assert - 100 > x.Value means x.Value < 100
        Assert.That(analysis.RangeFilters, Contains.Key("Value"));
        var filter = analysis.RangeFilters["Value"];
        Assert.That(filter.Max, Is.EqualTo(100));
        Assert.That(filter.MaxInclusive, Is.False);
    }

    #endregion

    #region Different Data Type Tests

    [Test]
    public void RangeFilter_LongType_ExtractsCorrectly()
    {
        // Arrange
        CreateTestData();
        using var table = ParquetTable<RangeTestEntity>.Factory.FromFileSystem(_testDirectory);

        // Act - Real LINQ expression: x.Id >= 500L
        var query = table.Where(x => x.Id >= 500L);
        var expression = query.Expression;
        var analysis = QueryAnalyzer.Analyze(expression);

        // Assert
        Assert.That(analysis.RangeFilters, Contains.Key("Id"));
        var filter = analysis.RangeFilters["Id"];
        Assert.That(filter.Min, Is.EqualTo(500L));
        Assert.That(filter.MinInclusive, Is.True);
    }

    [Test]
    public void RangeFilter_DoubleType_ExtractsCorrectly()
    {
        // Arrange
        CreateTestDataWithDoubles();
        using var table = ParquetTable<RangeTestEntityWithDouble>.Factory.FromFileSystem(_testDirectory);

        // Act - Real LINQ expression: x.Price >= 10.5
        var query = table.Where(x => x.Price >= 10.5);
        var expression = query.Expression;
        var analysis = QueryAnalyzer.Analyze(expression);

        // Assert
        Assert.That(analysis.RangeFilters, Contains.Key("Price"));
        var filter = analysis.RangeFilters["Price"];
        Assert.That(filter.Min, Is.EqualTo(10.5));
        Assert.That(filter.MinInclusive, Is.True);
    }

    [Test]
    public void RangeFilter_DateTimeType_ExtractsCorrectly()
    {
        // Arrange
        CreateTestDataWithDates();
        using var table = ParquetTable<RangeTestEntityWithDate>.Factory.FromFileSystem(_testDirectory);

        var cutoffDate = new DateTime(2024, 6, 1);

        // Act - Real LINQ expression: x.EventDate >= cutoffDate
        var query = table.Where(x => x.EventDate >= cutoffDate);
        var expression = query.Expression;
        var analysis = QueryAnalyzer.Analyze(expression);

        // Assert
        Assert.That(analysis.RangeFilters, Contains.Key("EventDate"));
        var filter = analysis.RangeFilters["EventDate"];
        Assert.That(filter.Min, Is.EqualTo(cutoffDate));
        Assert.That(filter.MinInclusive, Is.True);
    }

    [Test]
    public void RangeFilter_DateOnlyType_ExtractsCorrectly()
    {
        // Arrange
        CreateTestDataWithDateOnly();
        using var table = ParquetTable<RangeTestEntityWithDateOnly>.Factory.FromFileSystem(_testDirectory);

        var cutoffDate = new DateOnly(2024, 6, 1);

        // Act - Real LINQ expression: x.EventDate >= cutoffDate
        var query = table.Where(x => x.EventDate >= cutoffDate);
        var expression = query.Expression;
        var analysis = QueryAnalyzer.Analyze(expression);

        // Assert
        Assert.That(analysis.RangeFilters, Contains.Key("EventDate"));
        var filter = analysis.RangeFilters["EventDate"];
        Assert.That(filter.Min, Is.EqualTo(cutoffDate));
        Assert.That(filter.MinInclusive, Is.True);
    }

    [Test]
    public void RangeFilter_StringCompare_GreaterThanOrEqual_ExtractsCorrectly()
    {
        // Arrange
        CreateTestDataWithStrings();
        using var table = ParquetTable<RangeTestEntityWithString>.Factory.FromFileSystem(_testDirectory);

        var minName = "Bob";

        // Act - Real LINQ expression: string.Compare(x.Name, minName) >= 0
        var query = table.Where(x => string.Compare(x.Name, minName, StringComparison.Ordinal) >= 0);
        var expression = query.Expression;
        var analysis = QueryAnalyzer.Analyze(expression);

        // Assert - string.Compare(x.Name, "Bob") >= 0 means x.Name >= "Bob"
        Assert.That(analysis.RangeFilters, Contains.Key("Name"));
        var filter = analysis.RangeFilters["Name"];
        Assert.That(filter.Min, Is.EqualTo("Bob"));
        Assert.That(filter.MinInclusive, Is.True);
        Assert.That(filter.Max, Is.Null, "No upper bound");
    }

    [Test]
    public void RangeFilter_StringCompare_LessThan_ExtractsCorrectly()
    {
        // Arrange
        CreateTestDataWithStrings();
        using var table = ParquetTable<RangeTestEntityWithString>.Factory.FromFileSystem(_testDirectory);

        var maxName = "Charlie";

        // Act - Real LINQ expression: string.Compare(x.Name, maxName) < 0
        var query = table.Where(x => string.Compare(x.Name, maxName, StringComparison.Ordinal) < 0);
        var expression = query.Expression;
        var analysis = QueryAnalyzer.Analyze(expression);

        // Assert - string.Compare(x.Name, "Charlie") < 0 means x.Name < "Charlie"
        Assert.That(analysis.RangeFilters, Contains.Key("Name"));
        var filter = analysis.RangeFilters["Name"];
        Assert.That(filter.Min, Is.Null, "No lower bound");
        Assert.That(filter.Max, Is.EqualTo("Charlie"));
        Assert.That(filter.MaxInclusive, Is.False);
    }

    [Test]
    public void RangeFilter_StringCompare_Equal_ExtractsCorrectly()
    {
        // Arrange
        CreateTestDataWithStrings();
        using var table = ParquetTable<RangeTestEntityWithString>.Factory.FromFileSystem(_testDirectory);

        var targetName = "Bob";

        // Act - Real LINQ expression: string.Compare(x.Name, targetName) == 0
        var query = table.Where(x => string.Compare(x.Name, targetName, StringComparison.Ordinal) == 0);
        var expression = query.Expression;
        var analysis = QueryAnalyzer.Analyze(expression);

        // Assert - string.Compare(x.Name, "Bob") == 0 means x.Name == "Bob"
        Assert.That(analysis.RangeFilters, Contains.Key("Name"));
        var filter = analysis.RangeFilters["Name"];
        Assert.That(filter.Min, Is.EqualTo("Bob"));
        Assert.That(filter.Max, Is.EqualTo("Bob"));
        Assert.That(filter.MinInclusive, Is.True);
        Assert.That(filter.MaxInclusive, Is.True);
    }

    #endregion

    #region Complex Query Tests

    [Test]
    public void RangeFilter_WithSelectProjection_ExtractsCorrectly()
    {
        // Arrange
        CreateTestData();
        using var table = ParquetTable<RangeTestEntity>.Factory.FromFileSystem(_testDirectory);

        // Act - Real LINQ expression: Where + Select
        var query = table.Where(x => x.Value >= 50).Select(x => new { x.Id, x.Value });
        var expression = query.Expression;
        var analysis = QueryAnalyzer.Analyze(expression);

        // Assert
        Assert.That(analysis.RangeFilters, Contains.Key("Value"));
        var filter = analysis.RangeFilters["Value"];
        Assert.That(filter.Min, Is.EqualTo(50));
    }

    [Test]
    public void RangeFilter_ChainedWhereClauses_CombinesFilters()
    {
        // Arrange
        CreateTestData();
        using var table = ParquetTable<RangeTestEntity>.Factory.FromFileSystem(_testDirectory);

        // Act - Real LINQ expression: chained Where clauses
        var query = table.Where(x => x.Value >= 50).Where(x => x.Value < 100);
        var expression = query.Expression;
        var analysis = QueryAnalyzer.Analyze(expression);

        // Assert
        Assert.That(analysis.RangeFilters, Contains.Key("Value"));
        var filter = analysis.RangeFilters["Value"];
        Assert.That(filter.Min, Is.EqualTo(50));
        Assert.That(filter.Max, Is.EqualTo(100));
    }

    [Test]
    public void RangeFilter_WithCount_ExtractsCorrectly()
    {
        // Arrange
        CreateTestData();
        using var table = ParquetTable<RangeTestEntity>.Factory.FromFileSystem(_testDirectory);

        // Act - Real LINQ expression: Count with predicate
        var queryable = table.Where(x => x.Value >= 50);
        var expression = queryable.Expression;
        var analysis = QueryAnalyzer.Analyze(expression);

        // Assert
        Assert.That(analysis.RangeFilters, Contains.Key("Value"));
        var filter = analysis.RangeFilters["Value"];
        Assert.That(filter.Min, Is.EqualTo(50));
    }

    #endregion

    #region Variable Capture Tests

    [Test]
    public void RangeFilter_WithVariableCapture_ExtractsCorrectly()
    {
        // Arrange
        CreateTestData();
        using var table = ParquetTable<RangeTestEntity>.Factory.FromFileSystem(_testDirectory);

        var minValue = 50;
        var maxValue = 100;

        // Act - Real LINQ expression: using captured variables
        var query = table.Where(x => x.Value >= minValue && x.Value < maxValue);
        var expression = query.Expression;
        var analysis = QueryAnalyzer.Analyze(expression);

        // Assert
        Assert.That(analysis.RangeFilters, Contains.Key("Value"));
        var filter = analysis.RangeFilters["Value"];
        Assert.That(filter.Min, Is.EqualTo(50));
        Assert.That(filter.Max, Is.EqualTo(100));
    }

    [Test]
    public void RangeFilter_WithComputedValue_ExtractsCorrectly()
    {
        // Arrange
        CreateTestData();
        using var table = ParquetTable<RangeTestEntity>.Factory.FromFileSystem(_testDirectory);

        var baseValue = 25;

        // Act - Real LINQ expression: using computed value
        var query = table.Where(x => x.Value >= baseValue * 2);
        var expression = query.Expression;
        var analysis = QueryAnalyzer.Analyze(expression);

        // Assert
        Assert.That(analysis.RangeFilters, Contains.Key("Value"));
        var filter = analysis.RangeFilters["Value"];
        Assert.That(filter.Min, Is.EqualTo(50));
    }

    #endregion

    #region Edge Cases

    [Test]
    public void RangeFilter_NoRangeFilters_ReturnsEmptyDictionary()
    {
        // Arrange
        CreateTestData();
        using var table = ParquetTable<RangeTestEntity>.Factory.FromFileSystem(_testDirectory);

        // Act - Real LINQ expression: just Select, no Where
        var query = table.Select(x => x.Id);
        var expression = query.Expression;
        var analysis = QueryAnalyzer.Analyze(expression);

        // Assert
        Assert.That(analysis.RangeFilters, Is.Empty);
    }

    [Test]
    public void RangeFilter_BooleanExpression_DoesNotCreateRangeFilter()
    {
        // Arrange
        CreateTestDataWithBool();
        using var table = ParquetTable<RangeTestEntityWithBool>.Factory.FromFileSystem(_testDirectory);

        // Act - Real LINQ expression: boolean property
        var query = table.Where(x => x.IsActive);
        var expression = query.Expression;
        var analysis = QueryAnalyzer.Analyze(expression);

        // Assert - Booleans shouldn't create range filters
        Assert.That(analysis.RangeFilters, Is.Empty);
    }

    #endregion

    #region Helper Methods

    private void CreateTestData()
    {
        var path = Path.Combine(_testDirectory, "data.parquet");
        using var writer = new ParquetFileWriter(path,
        [
            new Column<long>("id"), new Column<int>("value")
        ]);

        using var rowGroup = writer.AppendRowGroup();
        using (var idWriter = rowGroup.NextColumn().LogicalWriter<long>())
        {
            idWriter.WriteBatch(new long[] { 1, 2, 3, 4, 5 });
        }

        using (var valueWriter = rowGroup.NextColumn().LogicalWriter<int>())
        {
            valueWriter.WriteBatch(new[] { 10, 50, 75, 100, 150 });
        }
    }

    private void CreateTestDataWithDoubles()
    {
        var path = Path.Combine(_testDirectory, "data.parquet");
        using var writer = new ParquetFileWriter(path,
        [
            new Column<long>("id"),
            new Column<double>("price")
        ]);

        using var rowGroup = writer.AppendRowGroup();
        using (var idWriter = rowGroup.NextColumn().LogicalWriter<long>())
        {
            idWriter.WriteBatch(new long[] { 1, 2, 3 });
        }

        using (var priceWriter = rowGroup.NextColumn().LogicalWriter<double>())
        {
            priceWriter.WriteBatch(new[] { 5.5, 10.5, 20.5 });
        }
    }

    private void CreateTestDataWithDates()
    {
        var path = Path.Combine(_testDirectory, "data.parquet");
        using var writer = new ParquetFileWriter(path,
        [
            new Column<long>("id"),
            new Column<DateTime>("event_date")
        ]);

        using var rowGroup = writer.AppendRowGroup();
        using (var idWriter = rowGroup.NextColumn().LogicalWriter<long>())
        {
            idWriter.WriteBatch(new long[] { 1, 2, 3 });
        }

        using (var dateWriter = rowGroup.NextColumn().LogicalWriter<DateTime>())
        {
            dateWriter.WriteBatch(new DateTime[]
            {
                new(2024, 1, 1),
                new(2024, 6, 1),
                new(2024, 12, 31)
            });
        }
    }

    private void CreateTestDataWithDateOnly()
    {
        var path = Path.Combine(_testDirectory, "data.parquet");
        using var writer = new ParquetFileWriter(path,
        [
            new Column<long>("id"),
            new Column<DateOnly>("event_date", LogicalType.Date())
        ]);

        using var rowGroup = writer.AppendRowGroup();
        using (var idWriter = rowGroup.NextColumn().LogicalWriter<long>())
        {
            idWriter.WriteBatch(new long[] { 1, 2, 3 });
        }

        using (var dateWriter = rowGroup.NextColumn().LogicalWriter<DateOnly>())
        {
            dateWriter.WriteBatch(new DateOnly[]
            {
                new(2024, 1, 1),
                new(2024, 6, 1),
                new(2024, 12, 31)
            });
        }
    }

    private void CreateTestDataWithStrings()
    {
        var path = Path.Combine(_testDirectory, "data.parquet");
        using var writer = new ParquetFileWriter(path,
        [
            new Column<long>("id"),
            new Column<string>("name")
        ]);

        using var rowGroup = writer.AppendRowGroup();
        using (var idWriter = rowGroup.NextColumn().LogicalWriter<long>())
        {
            idWriter.WriteBatch(new long[] { 1, 2, 3 });
        }

        using (var nameWriter = rowGroup.NextColumn().LogicalWriter<string>())
        {
            nameWriter.WriteBatch(new[] { "Alice", "Bob", "Charlie" });
        }
    }

    private void CreateTestDataWithBool()
    {
        var path = Path.Combine(_testDirectory, "data.parquet");
        using var writer = new ParquetFileWriter(path,
        [
            new Column<long>("id"),
            new Column<bool>("is_active")
        ]);

        using var rowGroup = writer.AppendRowGroup();
        using (var idWriter = rowGroup.NextColumn().LogicalWriter<long>())
        {
            idWriter.WriteBatch(new long[] { 1, 2, 3 });
        }

        using (var activeWriter = rowGroup.NextColumn().LogicalWriter<bool>())
        {
            activeWriter.WriteBatch(new[] { true, false, true });
        }
    }

    #endregion
}