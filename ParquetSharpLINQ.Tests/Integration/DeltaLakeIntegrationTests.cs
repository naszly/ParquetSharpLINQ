using ParquetSharpLINQ.Attributes;

namespace ParquetSharpLINQ.Tests.Integration;

[TestFixture]
[Category("Integration")]
public class DeltaLakeIntegrationTests
{
    private static readonly string TestDataPath = Path.Combine(
        TestContext.CurrentContext.TestDirectory,
        "..", "..", "..", "..",
        "ParquetSharpLINQ.Tests", "Integration", "delta_test_data");

    [OneTimeSetUp]
    public void OneTimeSetUp()
    {
        if (!Directory.Exists(TestDataPath))
        {
            Assert.Inconclusive(
                $"Delta test data not found at {TestDataPath}. " +
                "Run 'python3 Integration/generate_delta_test_data.py' to generate test data.");
        }
    }

    [Test]
    public void SimpleDeltaTable_CanBeQueried()
    {
        var tablePath = Path.Combine(TestDataPath, "simple_delta");
        
        if (!Directory.Exists(tablePath))
        {
            Assert.Inconclusive("Simple delta table not found. Generate test data first.");
        }

        using var table = new HiveParquetTable<SimpleDeltaRecord>(tablePath);

        var results = table.ToList();

        Assert.That(results, Has.Count.EqualTo(5));
        Assert.That(results.Select(r => r.Name), Contains.Item("Alice"));
        Assert.That(results.Select(r => r.Name), Contains.Item("Bob"));
    }

    [Test]
    public void SimpleDeltaTable_FilterWorks()
    {
        var tablePath = Path.Combine(TestDataPath, "simple_delta");
        
        if (!Directory.Exists(tablePath))
        {
            Assert.Inconclusive("Simple delta table not found.");
        }

        using var table = new HiveParquetTable<SimpleDeltaRecord>(tablePath);

        var results = table.Where(r => r.Amount > 150).ToList();

        Assert.That(results, Has.Count.EqualTo(4));
        Assert.That(results.All(r => r.Amount > 150), Is.True);
    }

    [Test]
    public void PartitionedDeltaTable_DiscoverAllPartitions()
    {
        var tablePath = Path.Combine(TestDataPath, "partitioned_delta");
        
        if (!Directory.Exists(tablePath))
        {
            Assert.Inconclusive("Partitioned delta table not found.");
        }

        using var table = new HiveParquetTable<PartitionedDeltaRecord>(tablePath);

        var partitions = table.DiscoverPartitions().ToList();

        Assert.That(partitions, Is.Not.Empty);
        Assert.That(partitions.Count, Is.EqualTo(24)); // 2 years * 12 months
    }

    [Test]
    public void PartitionedDeltaTable_PartitionPruningWorks()
    {
        var tablePath = Path.Combine(TestDataPath, "partitioned_delta");
        
        if (!Directory.Exists(tablePath))
        {
            Assert.Inconclusive("Partitioned delta table not found.");
        }

        using var table = new HiveParquetTable<PartitionedDeltaRecord>(tablePath);

        var results = table
            .Where(r => r.Year == 2024 && r.Month == 6)
            .ToList();

        Assert.That(results, Is.Not.Empty);
        Assert.That(results.All(r => r.Year == 2024), Is.True);
        Assert.That(results.All(r => r.Month == 6), Is.True);
        Assert.That(results, Has.Count.EqualTo(5)); // 5 records per month
    }

    [Test]
    public void DeltaTableWithUpdates_ReadsLatestVersion()
    {
        var tablePath = Path.Combine(TestDataPath, "delta_with_updates");
        
        if (!Directory.Exists(tablePath))
        {
            Assert.Inconclusive("Delta table with updates not found.");
        }

        using var table = new HiveParquetTable<DeltaProductRecord>(tablePath);

        var results = table.ToList();

        // Should have 6 records after updates and deletes
        // Initial: 5, Updated: 2, Deleted: 1, Added: 2 = 6 total
        Assert.That(results, Has.Count.EqualTo(6));

        // Check that deleted record (id=5) is not present
        Assert.That(results.All(r => r.Id != 5), Is.True);

        // Check that updated records have new values
        var productB = results.FirstOrDefault(r => r.Id == 2);
        Assert.That(productB, Is.Not.Null);
        Assert.That(productB!.Name, Does.Contain("Updated"));

        // Check that new records (id=6,7) are present
        Assert.That(results.Any(r => r.Id == 6), Is.True);
        Assert.That(results.Any(r => r.Id == 7), Is.True);
    }

    [Test]
    public void DeltaTableWithStringPartitions_FilterByRegion()
    {
        var tablePath = Path.Combine(TestDataPath, "delta_string_partitions");
        
        if (!Directory.Exists(tablePath))
        {
            Assert.Inconclusive("Delta table with string partitions not found.");
        }

        using var table = new HiveParquetTable<DeltaOrderRecord>(tablePath);

        var results = table
            .Where(r => r.Region == "eu-west")
            .ToList();

        Assert.That(results, Has.Count.EqualTo(10));
        Assert.That(results.All(r => r.Region == "eu-west"), Is.True);
    }

    [Test]
    public void DeltaTableWithStringPartitions_MultipleRegionFilter()
    {
        var tablePath = Path.Combine(TestDataPath, "delta_string_partitions");
        
        if (!Directory.Exists(tablePath))
        {
            Assert.Inconclusive("Delta table with string partitions not found.");
        }

        using var table = new HiveParquetTable<DeltaOrderRecord>(tablePath);

        var results = table
            .Where(r => r.Region == "us-east" || r.Region == "us-west")
            .ToList();

        Assert.That(results, Has.Count.EqualTo(20));
        Assert.That(results.All(r => r.Region.StartsWith("us-")), Is.True);
    }

    [Test]
    public void DeltaTable_CountWorks()
    {
        var tablePath = Path.Combine(TestDataPath, "simple_delta");
        
        if (!Directory.Exists(tablePath))
        {
            Assert.Inconclusive("Simple delta table not found.");
        }

        using var table = new HiveParquetTable<SimpleDeltaRecord>(tablePath);

        var count = table.Count();

        Assert.That(count, Is.EqualTo(5));
    }

    [Test]
    public void DeltaTable_AggregationsWork()
    {
        var tablePath = Path.Combine(TestDataPath, "simple_delta");
        
        if (!Directory.Exists(tablePath))
        {
            Assert.Inconclusive("Simple delta table not found.");
        }

        using var table = new HiveParquetTable<SimpleDeltaRecord>(tablePath);

        var totalAmount = table.Sum(r => r.Amount);
        var avgAmount = table.Average(r => r.Amount);
        var maxAmount = table.Max(r => r.Amount);

        Assert.That(totalAmount, Is.GreaterThan(0));
        Assert.That(avgAmount, Is.GreaterThan(0));
        Assert.That(maxAmount, Is.EqualTo(300.0));
    }

    [Test]
    public void DeltaLog_ExistsAndContainsJsonFiles()
    {
        var tablePath = Path.Combine(TestDataPath, "simple_delta");
        var deltaLogPath = Path.Combine(tablePath, "_delta_log");
        
        Assert.That(Directory.Exists(deltaLogPath), Is.True, 
            "Delta log directory should exist");

        var jsonFiles = Directory.GetFiles(deltaLogPath, "*.json");
        
        Assert.That(jsonFiles, Is.Not.Empty, 
            "Delta log should contain JSON transaction log files");
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

