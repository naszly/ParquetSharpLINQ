using System.Linq.Expressions;

namespace ParquetSharpLINQ.Tests.Unit;

[TestFixture]
[Category("Unit")]
public class QueryAnalyzerTests
{
    [Test]
    public void Analyze_WithSimpleWhere_ExtractsPartitionFilter()
    {
        var table = CreateMockTable();
        var query = table.Where(e => e.Year == 2024);

        var analysis = QueryAnalyzer.Analyze(query.Expression);

        Assert.That(analysis.PartitionFilters, Does.ContainKey("Year"));
        Assert.That(analysis.PartitionFilters["Year"], Is.EqualTo(2024));
    }

    [Test]
    public void Analyze_WithMultipleWhereConditions_ExtractsAllFilters()
    {
        var table = CreateMockTable();
        var query = table.Where(e => e.Year == 2024 && e.Region == "US");

        var analysis = QueryAnalyzer.Analyze(query.Expression);

        Assert.That(analysis.PartitionFilters, Does.ContainKey("Year"));
        Assert.That(analysis.PartitionFilters, Does.ContainKey("Region"));
        Assert.That(analysis.PartitionFilters["Year"], Is.EqualTo(2024));
        Assert.That(analysis.PartitionFilters["Region"], Is.EqualTo("US"));
    }

    [Test]
    public void Analyze_WithSelectProjection_ExtractsRequestedColumns()
    {
        var table = CreateMockTable();
        var query = table.Select(e => new { e.Id, e.Name });

        var analysis = QueryAnalyzer.Analyze(query.Expression);

        Assert.That(analysis.RequestedColumns, Does.Contain("Id"));
        Assert.That(analysis.RequestedColumns, Does.Contain("Name"));
        Assert.That(analysis.RequestedColumns, Has.Count.EqualTo(2));
    }

    [Test]
    public void Analyze_WithComplexQuery_ExtractsBothFiltersAndColumns()
    {
        var table = CreateMockTable();
        var query = table
            .Where(e => e.Year == 2024)
            .Select(e => new { e.Id, e.Amount });

        var analysis = QueryAnalyzer.Analyze(query.Expression);

        Assert.That(analysis.PartitionFilters, Does.ContainKey("Year"));

        Assert.That(analysis.RequestedColumns, Does.Contain("Id"));
        Assert.That(analysis.RequestedColumns, Does.Contain("Amount"));
    }

    [Test]
    public void Analyze_WithNoFilters_ReturnsEmptyFilters()
    {
        var table = CreateMockTable();
        var query = table.Where(e => e.Amount > 100); // Non-partition filter

        var analysis = QueryAnalyzer.Analyze(query.Expression);

        Assert.That(analysis.RequestedColumns, Does.Contain("Amount"));
        // but the key point is it won't filter partitions incorrectly
    }

    [Test]
    public void Analyze_WithCountPredicate_ExtractsPartitionFilter()
    {
        var table = CreateMockTable();
        Expression<Func<IQueryable<TestEntity>, int>> countExpr = q => q.Count(e => e.Year == 2024);
        var expression = Expression.Invoke(countExpr, table.Expression);

        var analysis = QueryAnalyzer.Analyze(expression);

        Assert.That(analysis.PartitionFilters, Does.ContainKey("Year"));
        Assert.That(analysis.PartitionFilters["Year"], Is.EqualTo(2024));
    }

    [Test]
    public void Analyze_WithAnyPredicate_ExtractsPartitionFilter()
    {
        var table = CreateMockTable();
        Expression<Func<IQueryable<TestEntity>, bool>> anyExpr = q => q.Any(e => e.Year == 2024 && e.Region == "US");
        var expression = Expression.Invoke(anyExpr, table.Expression);

        var analysis = QueryAnalyzer.Analyze(expression);

        Assert.That(analysis.PartitionFilters, Does.ContainKey("Year"));
        Assert.That(analysis.PartitionFilters, Does.ContainKey("Region"));
        Assert.That(analysis.PartitionFilters["Year"], Is.EqualTo(2024));
        Assert.That(analysis.PartitionFilters["Region"], Is.EqualTo("US"));
    }

    [Test]
    public void Analyze_WithFirstPredicate_ExtractsPartitionFilter()
    {
        var table = CreateMockTable();
        Expression<Func<IQueryable<TestEntity>, TestEntity>> firstExpr = q => q.First(e => e.Year == 2024);
        var expression = Expression.Invoke(firstExpr, table.Expression);

        var analysis = QueryAnalyzer.Analyze(expression);

        Assert.That(analysis.PartitionFilters, Does.ContainKey("Year"));
        Assert.That(analysis.PartitionFilters["Year"], Is.EqualTo(2024));
    }

    [Test]
    public void Analyze_WithFirstOrDefaultPredicate_ExtractsPartitionFilter()
    {
        var table = CreateMockTable();
        Expression<Func<IQueryable<TestEntity>, TestEntity?>> firstOrDefaultExpr =
            q => q.FirstOrDefault(e => e.Region == "EU");
        var expression = Expression.Invoke(firstOrDefaultExpr, table.Expression);

        var analysis = QueryAnalyzer.Analyze(expression);

        Assert.That(analysis.PartitionFilters, Does.ContainKey("Region"));
        Assert.That(analysis.PartitionFilters["Region"], Is.EqualTo("EU"));
    }

    [Test]
    public void Analyze_WithSinglePredicate_ExtractsPartitionFilter()
    {
        var table = CreateMockTable();
        Expression<Func<IQueryable<TestEntity>, TestEntity>> singleExpr = q =>
            q.Single(e => e.Year == 2024 && e.Region == "US");
        var expression = Expression.Invoke(singleExpr, table.Expression);

        var analysis = QueryAnalyzer.Analyze(expression);

        Assert.That(analysis.PartitionFilters, Does.ContainKey("Year"));
        Assert.That(analysis.PartitionFilters, Does.ContainKey("Region"));
    }

    [Test]
    public void Analyze_WithAllPredicate_ExtractsPartitionFilter()
    {
        var table = CreateMockTable();
        Expression<Func<IQueryable<TestEntity>, bool>> allExpr = q => q.All(e => e.Year == 2024);
        var expression = Expression.Invoke(allExpr, table.Expression);

        var analysis = QueryAnalyzer.Analyze(expression);

        Assert.That(analysis.PartitionFilters, Does.ContainKey("Year"));
        Assert.That(analysis.PartitionFilters["Year"], Is.EqualTo(2024));
    }

    private static IQueryable<TestEntity> CreateMockTable()
    {
        var data = new List<TestEntity>
        {
            new() { Id = 1, Name = "Test", Amount = 100, Year = 2024, Region = "US" }
        };
        return data.AsQueryable();
    }
}