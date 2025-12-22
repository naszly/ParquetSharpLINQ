using System.Linq.Expressions;
using ParquetSharpLINQ.Query;

namespace ParquetSharpLINQ.Tests.Unit.Query;

[TestFixture]
[Category("Unit")]
[Category("Query")]
public class QueryAnalyzerTests
{
    [Test]
    public void Analyze_WithSimpleWhere_StoresPredicateAndRangeFilter()
    {
        var table = CreateMockTable();
        var query = table.Where(e => e.Year == 2024);

        var analysis = QueryAnalyzer.Analyze(query.Expression);

        Assert.That(analysis.Predicates, Has.Count.EqualTo(1));
        Assert.That(analysis.RangeFilters, Does.ContainKey("Year"));
        Assert.That(analysis.RangeFilters["Year"].Min, Is.EqualTo(2024));
        Assert.That(analysis.RangeFilters["Year"].Max, Is.EqualTo(2024));
    }

    [Test]
    public void Analyze_WithMultipleWhereConditions_StoresRangeFilters()
    {
        var table = CreateMockTable();
        var query = table.Where(e => e.Year == 2024 && e.Region == "US");

        var analysis = QueryAnalyzer.Analyze(query.Expression);

        Assert.That(analysis.Predicates, Has.Count.EqualTo(1));
        Assert.That(analysis.RangeFilters, Does.ContainKey("Year"));
        Assert.That(analysis.RangeFilters, Does.ContainKey("Region"));
        Assert.That(analysis.RangeFilters["Year"].Min, Is.EqualTo(2024));
        Assert.That(analysis.RangeFilters["Region"].Min, Is.EqualTo("US"));
    }

    [Test]
    public void Analyze_WithSelectProjection_ExtractsSelectedColumns()
    {
        var table = CreateMockTable();
        var query = table.Select(e => new { e.Id, e.Name });

        var analysis = QueryAnalyzer.Analyze(query.Expression);

        Assert.That(analysis.SelectedColumns, Does.Contain("Id"));
        Assert.That(analysis.SelectedColumns, Does.Contain("Name"));
        Assert.That(analysis.SelectedColumns, Has.Count.EqualTo(2));
    }

    [Test]
    public void Analyze_WithComplexQuery_ExtractsPredicatesAndColumns()
    {
        var table = CreateMockTable();
        var query = table
            .Where(e => e.Year == 2024)
            .Select(e => new { e.Id, e.Amount });

        var analysis = QueryAnalyzer.Analyze(query.Expression);

        Assert.That(analysis.Predicates, Has.Count.EqualTo(1));
        Assert.That(analysis.RangeFilters, Does.ContainKey("Year"));

        Assert.That(analysis.SelectedColumns, Does.Contain("Id"));
        Assert.That(analysis.SelectedColumns, Does.Contain("Amount"));
    }

    [Test]
    public void Analyze_WithNonPartitionFilter_StoresPredicate()
    {
        var table = CreateMockTable();
        var query = table.Where(e => e.Amount > 100); // Non-partition filter

        var analysis = QueryAnalyzer.Analyze(query.Expression);

        // SelectedColumns should be null when there's no SELECT projection
        Assert.That(analysis.SelectedColumns, Is.Null, 
            "SelectedColumns should be null when there's no SELECT projection");
        Assert.That(analysis.Predicates, Has.Count.EqualTo(1));
        Assert.That(analysis.RangeFilters, Does.ContainKey("Amount"));
    }

    [Test]
    public void Analyze_WithCountPredicate_StoresPredicate()
    {
        var table = CreateMockTable();
        Expression<Func<IQueryable<TestEntity>, int>> countExpr = q => q.Count(e => e.Year == 2024);
        var expression = Expression.Invoke(countExpr, table.Expression);

        var analysis = QueryAnalyzer.Analyze(expression);

        Assert.That(analysis.Predicates, Has.Count.EqualTo(1));
        Assert.That(analysis.RangeFilters, Does.ContainKey("Year"));
    }

    [Test]
    public void Analyze_WithAnyPredicate_StoresPredicates()
    {
        var table = CreateMockTable();
        Expression<Func<IQueryable<TestEntity>, bool>> anyExpr = q => q.Any(e => e.Year == 2024 && e.Region == "US");
        var expression = Expression.Invoke(anyExpr, table.Expression);

        var analysis = QueryAnalyzer.Analyze(expression);

        Assert.That(analysis.Predicates, Has.Count.EqualTo(1));
        Assert.That(analysis.RangeFilters, Does.ContainKey("Year"));
        Assert.That(analysis.RangeFilters, Does.ContainKey("Region"));
    }

    [Test]
    public void Analyze_WithFirstPredicate_StoresPredicate()
    {
        var table = CreateMockTable();
        Expression<Func<IQueryable<TestEntity>, TestEntity>> firstExpr = q => q.First(e => e.Year == 2024);
        var expression = Expression.Invoke(firstExpr, table.Expression);

        var analysis = QueryAnalyzer.Analyze(expression);

        Assert.That(analysis.Predicates, Has.Count.EqualTo(1));
        Assert.That(analysis.RangeFilters, Does.ContainKey("Year"));
    }

    [Test]
    public void Analyze_WithFirstOrDefaultPredicate_StoresPredicate()
    {
        var table = CreateMockTable();
        Expression<Func<IQueryable<TestEntity>, TestEntity?>> firstOrDefaultExpr =
            q => q.FirstOrDefault(e => e.Region == "EU");
        var expression = Expression.Invoke(firstOrDefaultExpr, table.Expression);

        var analysis = QueryAnalyzer.Analyze(expression);

        Assert.That(analysis.Predicates, Has.Count.EqualTo(1));
        Assert.That(analysis.RangeFilters, Does.ContainKey("Region"));
    }

    [Test]
    public void Analyze_WithSinglePredicate_StoresPredicate()
    {
        var table = CreateMockTable();
        Expression<Func<IQueryable<TestEntity>, TestEntity>> singleExpr = q =>
            q.Single(e => e.Year == 2024 && e.Region == "US");
        var expression = Expression.Invoke(singleExpr, table.Expression);

        var analysis = QueryAnalyzer.Analyze(expression);

        Assert.That(analysis.Predicates, Has.Count.EqualTo(1));
        Assert.That(analysis.RangeFilters, Does.ContainKey("Year"));
        Assert.That(analysis.RangeFilters, Does.ContainKey("Region"));
    }

    [Test]
    public void Analyze_WithAllPredicate_StoresPredicate()
    {
        var table = CreateMockTable();
        Expression<Func<IQueryable<TestEntity>, bool>> allExpr = q => q.All(e => e.Year == 2024);
        var expression = Expression.Invoke(allExpr, table.Expression);

        var analysis = QueryAnalyzer.Analyze(expression);

        Assert.That(analysis.Predicates, Has.Count.EqualTo(1));
        Assert.That(analysis.RangeFilters, Does.ContainKey("Year"));
    }

    [Test]
    public void Analyze_WithStringMethods_StartsWithChar_StoresPredicateWithoutRangeFilters()
    {
        var table = CreateMockTable();
        var query = table.Where(e => e.Region != null && e.Region.StartsWith('U'));

        var analysis = QueryAnalyzer.Analyze(query.Expression);

        Assert.That(analysis.Predicates, Has.Count.EqualTo(1));
        Assert.That(analysis.RangeFilters, Is.Empty);
    }

    [Test]
    public void Analyze_WithStringMethods_ContainsChar_StoresPredicateWithoutRangeFilters()
    {
        var table = CreateMockTable();
        var query = table.Where(e => e.Region != null && e.Region.Contains('S'));

        var analysis = QueryAnalyzer.Analyze(query.Expression);

        Assert.That(analysis.Predicates, Has.Count.EqualTo(1));
        Assert.That(analysis.RangeFilters, Is.Empty);
    }

    [Test]
    public void Analyze_WithStringMethods_StartsWithString_StoresPredicateWithoutRangeFilters()
    {
        var table = CreateMockTable();
        var query = table.Where(e => e.Region != null && e.Region.StartsWith("US"));

        var analysis = QueryAnalyzer.Analyze(query.Expression);

        Assert.That(analysis.Predicates, Has.Count.EqualTo(1));
        Assert.That(analysis.RangeFilters, Is.Empty);
    }

    [Test]
    public void Analyze_WithNotEqual_StoresPredicateWithoutRangeFilter()
    {
        var table = CreateMockTable();
        var query = table.Where(e => e.Year != 2024);

        var analysis = QueryAnalyzer.Analyze(query.Expression);

        Assert.That(analysis.Predicates, Has.Count.EqualTo(1));
        Assert.That(analysis.RangeFilters, Is.Empty);
    }

    [Test]
    public void Analyze_WithOrPredicate_StoresPredicateWithoutRangeFilters()
    {
        var table = CreateMockTable();
        var query = table.Where(e => e.Year == 2024 || e.Year == 2025);

        var analysis = QueryAnalyzer.Analyze(query.Expression);

        Assert.That(analysis.Predicates, Has.Count.EqualTo(1));
        Assert.That(analysis.RangeFilters, Is.Empty);
    }

    [Test]
    public void Analyze_WithSupportedStringMethods_DoesNotThrow()
    {
        var table = CreateMockTable();
        var predicates = new Expression<Func<TestEntity, bool>>[]
        {
            e => e.Region != null && e.Region.EndsWith("US"),
            e => e.Region != null && e.Region.EndsWith("US", StringComparison.OrdinalIgnoreCase),
            e => e.Region != null && e.Region.Equals("US"),
            e => e.Region != null && e.Region.Equals("US", StringComparison.OrdinalIgnoreCase),
            e => string.Equals(e.Region, "US", StringComparison.OrdinalIgnoreCase),
            e => e.Region != null && e.Region.ToLower() == "us",
            e => e.Region != null && e.Region.ToUpperInvariant() == "US",
            e => e.Region != null && e.Region.Trim() == "US",
            e => string.IsNullOrEmpty(e.Region),
            e => string.IsNullOrWhiteSpace(e.Region)
        };

        foreach (var predicate in predicates)
        {
            var query = table.Where(predicate);
            Assert.DoesNotThrow(() => QueryAnalyzer.Analyze(query.Expression));
        }
    }

    [Test]
    public void Analyze_WithNotContainsStringMethod_StoresPredicateWithoutRangeFilters()
    {
        var table = CreateMockTable();
        var query = table.Where(e => e.Region != null && !e.Region.Contains("US"));

        var analysis = QueryAnalyzer.Analyze(query.Expression);

        Assert.That(analysis.Predicates, Has.Count.EqualTo(1));
        Assert.That(analysis.RangeFilters, Is.Empty);
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
