using ParquetSharpLINQ.Attributes;

namespace ParquetSharpLINQ.Tests.Integration.Delta;

/// <summary>
/// Base class for Delta Lake integration tests with shared test logic.
/// Subclasses provide table creation strategy (local vs Azure).
/// </summary>
public abstract class DeltaLakeTestsBase
{
    protected abstract ParquetTable<T> CreateTable<T>(string tableName) where T : new();

    [Test]
    public void SimpleDeltaTable_CanBeQueried()
    {
        using var table = CreateTable<SimpleDeltaRecord>("simple_delta");

        var results = table.ToList();

        Assert.That(results, Has.Count.EqualTo(5));
        Assert.That(results.Select(r => r.Name), Contains.Item("Alice"));
        Assert.That(results.Select(r => r.Name), Contains.Item("Bob"));
    }

    [Test]
    public void SimpleDeltaTable_FilterWorks()
    {
        using var table = CreateTable<SimpleDeltaRecord>("simple_delta");

        var results = table.Where(r => r.Amount > 150).ToList();

        Assert.That(results, Has.Count.EqualTo(4));
        Assert.That(results.All(r => r.Amount > 150), Is.True);
    }

    [Test]
    public void PartitionedDeltaTable_DiscoverAllPartitions()
    {
        using var table = CreateTable<PartitionedDeltaRecord>("partitioned_delta");

        var partitions = table.DiscoverPartitions().ToList();

        Assert.That(partitions, Is.Not.Empty);
        Assert.That(partitions.Count, Is.EqualTo(24));
    }

    [Test]
    public void PartitionedDeltaTable_PartitionPruningWorks()
    {
        using var table = CreateTable<PartitionedDeltaRecord>("partitioned_delta");

        var results = table
            .Where(r => r.Year == 2024 && r.Month == 6)
            .ToList();

        Assert.That(results, Is.Not.Empty);
        Assert.That(results.All(r => r.Year == 2024), Is.True);
        Assert.That(results.All(r => r.Month == 6), Is.True);
        Assert.That(results, Has.Count.EqualTo(5));
    }

    [Test]
    public void DeltaTableWithUpdates_ReadsLatestVersion()
    {
        using var table = CreateTable<DeltaProductRecord>("delta_with_updates");

        var results = table.ToList();

        Assert.That(results, Has.Count.EqualTo(6));
        Assert.That(results.All(r => r.Id != 5), Is.True);

        var productB = results.FirstOrDefault(r => r.Id == 2);
        Assert.That(productB, Is.Not.Null);
        Assert.That(productB!.Name, Does.Contain("Updated"));

        Assert.That(results.Any(r => r.Id == 6), Is.True);
        Assert.That(results.Any(r => r.Id == 7), Is.True);
    }

    [Test]
    public void DeltaTableWithStringPartitions_FilterByRegion()
    {
        using var table = CreateTable<DeltaOrderRecord>("delta_string_partitions");

        var results = table
            .Where(r => r.Region == "eu-west")
            .ToList();

        Assert.That(results, Has.Count.EqualTo(10));
        Assert.That(results.All(r => r.Region == "eu-west"), Is.True);
    }

    [Test]
    public void DeltaTableWithStringPartitions_MultipleRegionFilter()
    {
        using var table = CreateTable<DeltaOrderRecord>("delta_string_partitions");

        var results = table
            .Where(r => r.Region == "us-east" || r.Region == "us-west")
            .ToList();

        Assert.That(results, Has.Count.EqualTo(20));
        Assert.That(results.All(r => r.Region?.StartsWith("us-") ?? false), Is.True);
    }

    [Test]
    public void DeltaTable_CountWorks()
    {
        using var table = CreateTable<SimpleDeltaRecord>("simple_delta");

        var count = table.Count();

        Assert.That(count, Is.EqualTo(5));
    }

    [Test]
    public void DeltaTable_AggregationsWork()
    {
        using var table = CreateTable<SimpleDeltaRecord>("simple_delta");

        var totalAmount = table.Sum(r => r.Amount);
        var avgAmount = table.Average(r => r.Amount);
        var maxAmount = table.Max(r => r.Amount);

        Assert.That(totalAmount, Is.GreaterThan(0));
        Assert.That(avgAmount, Is.GreaterThan(0));
        Assert.That(maxAmount, Is.EqualTo(300.0));
    }
}

// Test entity classes

public class SimpleDeltaRecord
{
    [ParquetColumn("id")]
    public long Id { get; set; }

    [ParquetColumn("name")]
    public string? Name { get; set; }

    [ParquetColumn("amount")]
    public double Amount { get; set; }

    [ParquetColumn("date")]
    public string? Date { get; set; }
}

public class PartitionedDeltaRecord
{
    [ParquetColumn("id")]
    public long Id { get; set; }

    [ParquetColumn("name")]
    public string? Name { get; set; }

    [ParquetColumn("amount")]
    public double Amount { get; set; }

    [ParquetColumn("year", IsPartition = true)]
    public int Year { get; set; }

    [ParquetColumn("month", IsPartition = true)]
    public int Month { get; set; }

    [ParquetColumn("date")]
    public string? Date { get; set; }
}

public class DeltaProductRecord
{
    [ParquetColumn("id")]
    public long Id { get; set; }

    [ParquetColumn("name")]
    public string? Name { get; set; }

    [ParquetColumn("quantity")]
    public long Quantity { get; set; }

    [ParquetColumn("year", IsPartition = true)]
    public int Year { get; set; }
}

public class DeltaOrderRecord
{
    [ParquetColumn("id")]
    public long Id { get; set; }

    [ParquetColumn("order_name")]
    public string? OrderName { get; set; }

    [ParquetColumn("total")]
    public double Total { get; set; }

    [ParquetColumn("region", IsPartition = true)]
    public string? Region { get; set; }

    [ParquetColumn("year", IsPartition = true)]
    public int Year { get; set; }
}
