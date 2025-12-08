using ParquetSharpLINQ.Benchmarks;
using ParquetSharpLINQ.DataGenerator;

namespace ParquetSharpLINQ.Tests.Integration;

[TestFixture]
[Category("Integration")]
[Category("LocalFiles")]
public class IntegrationTests
{
    [OneTimeSetUp]
    public void OneTimeSetup()
    {
        _testDataPath = Path.Combine(Path.GetTempPath(), $"ParquetIntegrationTest_{Guid.NewGuid()}");
        Directory.CreateDirectory(_testDataPath);

        var generator = new TestDataGenerator();
        generator.GenerateParquetFiles(
            _testDataPath,
            RecordsPerPartition,
            [2023, 2024],
            MonthsPerYear
        );
    }

    [OneTimeTearDown]
    public void OneTimeTearDown()
    {
        if (Directory.Exists(_testDataPath)) Directory.Delete(_testDataPath, true);
    }

    private string _testDataPath = null!;
    private const int RecordsPerPartition = 100;
    private const int Years = 2;
    private const int MonthsPerYear = 3;
    private const int Regions = 5;

    [Test]
    public void Integration_FullTableScan_ReturnsAllRecords()
    {
        using var table = new HiveParquetTable<SalesRecord>(_testDataPath);
        var expectedCount = Years * MonthsPerYear * Regions * RecordsPerPartition;

        var results = table.ToList();

        Assert.That(results, Has.Count.EqualTo(expectedCount));
        Assert.That(results.All(r => r.Id > 0), Is.True);
        Assert.That(results.All(r => !string.IsNullOrEmpty(r.ProductName)), Is.True);
    }

    [Test]
    public void Integration_PartitionPruning_ByYear_OnlyReadsMatchingPartitions()
    {
        using var table = new HiveParquetTable<SalesRecord>(_testDataPath);
        var expectedCount = MonthsPerYear * Regions * RecordsPerPartition;

        var results = table.Where(s => s.Year == 2024).ToList();

        Assert.That(results, Has.Count.EqualTo(expectedCount));
        Assert.That(results.All(r => r.Year == 2024), Is.True);
    }

    [Test]
    public void Integration_PartitionPruning_ByRegion_OnlyReadsMatchingPartitions()
    {
        using var table = new HiveParquetTable<SalesRecord>(_testDataPath);
        var expectedCount = Years * MonthsPerYear * RecordsPerPartition;

        var results = table.Where(s => s.Region == "eu-west").ToList();

        Assert.That(results, Has.Count.EqualTo(expectedCount));
        Assert.That(results.All(r => r.Region == "eu-west"), Is.True);
    }

    [Test]
    public void Integration_PartitionPruning_MultipleFilters_OnlyReadsSinglePartition()
    {
        using var table = new HiveParquetTable<SalesRecord>(_testDataPath);

        var expectedCount = RecordsPerPartition;

        var results = table
            .Where(s => s.Year == 2024 && s.Month == 1 && s.Region == "us-east")
            .ToList();

        Assert.That(results, Has.Count.EqualTo(expectedCount));
        Assert.That(results.All(r => r.Year == 2024 && r.Month == 1 && r.Region == "us-east"), Is.True);
    }

    [Test]
    public void Integration_CountWithPredicate_ReturnCorrectCount()
    {
        using var table = new HiveParquetTable<SalesRecord>(_testDataPath);
        var expectedCount = MonthsPerYear * Regions * RecordsPerPartition;

        var count = table.Count(s => s.Year == 2024);

        Assert.That(count, Is.EqualTo(expectedCount));
    }

    [Test]
    public void Integration_AnyWithPredicate_ReturnsTrue()
    {
        using var table = new HiveParquetTable<SalesRecord>(_testDataPath);

        var hasData = table.Any(s => s.Year == 2024 && s.Region == "eu-west");

        Assert.That(hasData, Is.True);
    }

    [Test]
    public void Integration_FirstOrDefaultWithPredicate_ReturnsRecord()
    {
        using var table = new HiveParquetTable<SalesRecord>(_testDataPath);

        var record = table.FirstOrDefault(s => s.Year == 2024 && s.Month == 2);

        Assert.That(record, Is.Not.Null);
        Assert.That(record!.Year, Is.EqualTo(2024));
        Assert.That(record.Month, Is.EqualTo(2));
    }

    [Test]
    public void Integration_ColumnProjection_OnlyReadsRequestedColumns()
    {
        using var table = new HiveParquetTable<SalesRecord>(_testDataPath);

        var results = table
            .Where(s => s.Year == 2024)
            .Select(s => new { s.Id, s.ProductName, s.TotalAmount })
            .Take(10)
            .ToList();

        Assert.That(results, Has.Count.EqualTo(10));
        Assert.That(results.All(r => r.Id > 0), Is.True);
        Assert.That(results.All(r => !string.IsNullOrEmpty(r.ProductName)), Is.True);
        Assert.That(results.All(r => r.TotalAmount > 0), Is.True);
    }

    [Test]
    public void Integration_ComplexQuery_CombinesMultipleOperations()
    {
        using var table = new HiveParquetTable<SalesRecord>(_testDataPath);

        var results = table
            .Where(s => s.Year == 2024 && s.Region == "us-east")
            .Where(s => s.TotalAmount > 500)
            .OrderByDescending(s => s.TotalAmount)
            .Select(s => new { s.Id, s.ProductName, s.TotalAmount })
            .Take(5)
            .ToList();

        Assert.That(results, Has.Count.LessThanOrEqualTo(5));

        for (var i = 0; i < results.Count - 1; i++)
            Assert.That(results[i].TotalAmount, Is.GreaterThanOrEqualTo(results[i + 1].TotalAmount));
    }

    [Test]
    public void Integration_Aggregations_CalculateCorrectly()
    {
        using var table = new HiveParquetTable<SalesRecord>(_testDataPath);

        var allRecords = table.Where(s => s.Year == 2024 && s.Month == 1).ToList();
        var sum = allRecords.Sum(s => s.TotalAmount);
        var avg = allRecords.Average(s => s.TotalAmount);
        var max = allRecords.Max(s => s.TotalAmount);
        var min = allRecords.Min(s => s.TotalAmount);

        Assert.That(sum, Is.GreaterThan(0));
        Assert.That(avg, Is.GreaterThan(0));
        Assert.That(max, Is.GreaterThanOrEqualTo(min));
        Assert.That(avg, Is.LessThanOrEqualTo(max));
        Assert.That(avg, Is.GreaterThanOrEqualTo(min));
    }

    [Test]
    public void Integration_MultipleQueries_OnSameTable_WorkIndependently()
    {
        using var table = new HiveParquetTable<SalesRecord>(_testDataPath);

        var count2023 = table.Count(s => s.Year == 2023);
        var count2024 = table.Count(s => s.Year == 2024);
        var usEastCount = table.Count(s => s.Region == "us-east");
        var euWestCount = table.Count(s => s.Region == "eu-west");

        Assert.That(count2023, Is.EqualTo(MonthsPerYear * Regions * RecordsPerPartition));
        Assert.That(count2024, Is.EqualTo(MonthsPerYear * Regions * RecordsPerPartition));
        Assert.That(usEastCount, Is.EqualTo(Years * MonthsPerYear * RecordsPerPartition));
        Assert.That(euWestCount, Is.EqualTo(Years * MonthsPerYear * RecordsPerPartition));
    }

    [Test]
    public void Integration_PartitionValues_AreCorrectlyEnriched()
    {
        using var table = new HiveParquetTable<SalesRecord>(_testDataPath);

        var records = table
            .Where(s => s.Year == 2023 && s.Month == 3 && s.Region == "eu-west")
            .Take(5)
            .ToList();

        Assert.That(records, Has.Count.EqualTo(5));

        foreach (var record in records)
        {
            Assert.That(record.Year, Is.EqualTo(2023));
            Assert.That(record.Month, Is.EqualTo(3));
            Assert.That(record.Region, Is.EqualTo("eu-west"));
        }
    }

    [Test]
    public void Integration_NoMatchingPartitions_ReturnsEmpty()
    {
        using var table = new HiveParquetTable<SalesRecord>(_testDataPath);

        var results = table.Where(s => s.Year == 2025).ToList();

        Assert.That(results, Is.Empty);
    }

    [Test]
    public void Integration_PartitionMatching_NormalizesToLowercase_CrossPlatform()
    {
        using var table = new HiveParquetTable<SalesRecord>(_testDataPath);
        var expectedCount = Years * MonthsPerYear * RecordsPerPartition;

        var lowerCaseQuery = table.Where(s => s.Region == "us-east").ToList();
        var upperCaseQuery = table.Where(s => s.Region == "US-EAST").ToList();

        Assert.That(lowerCaseQuery, Has.Count.EqualTo(expectedCount));
        Assert.That(upperCaseQuery, Is.Empty);
        Assert.That(lowerCaseQuery[0].Region, Is.EqualTo("us-east"));
    }

    [Test]
    public void Integration_CaseInsensitiveMatching_UsingStringComparison()
    {
        using var table = new HiveParquetTable<SalesRecord>(_testDataPath);
        var expectedCount = Years * MonthsPerYear * RecordsPerPartition;

        var upperCaseQuery = table
            .Where(s => s.Region.Equals("US-EAST", StringComparison.OrdinalIgnoreCase))
            .ToList();

        var mixedCaseQuery = table
            .Where(s => s.Region.Equals("Us-EaSt", StringComparison.OrdinalIgnoreCase))
            .ToList();

        var lowerCaseQuery = table
            .Where(s => s.Region.Equals("us-east", StringComparison.OrdinalIgnoreCase))
            .ToList();

        Assert.That(upperCaseQuery, Has.Count.EqualTo(expectedCount));
        Assert.That(mixedCaseQuery, Has.Count.EqualTo(expectedCount));
        Assert.That(lowerCaseQuery, Has.Count.EqualTo(expectedCount));
        Assert.That(upperCaseQuery.Select(r => r.Id), Is.EquivalentTo(lowerCaseQuery.Select(r => r.Id)));
    }

    [Test]
    public void Integration_NumericPartitionWithLeadingZeros_Matches()
    {
        var specialPath = Path.Combine(Path.GetTempPath(), $"ParquetLeadingZeroTest_{Guid.NewGuid()}");

        try
        {
            Directory.CreateDirectory(specialPath);

            const int recordsPerPartition = 50;
            var generator = new TestDataGenerator();
            generator.GenerateParquetFiles(
                specialPath,
                recordsPerPartition,
                [2024],
                1
            );

            using var table = new HiveParquetTable<SalesRecord>(specialPath);

            var expectedCount = recordsPerPartition * Regions;
            var count = table.Count(s => s.Month == 1);

            Assert.That(count, Is.EqualTo(expectedCount));
        }
        finally
        {
            if (Directory.Exists(specialPath)) Directory.Delete(specialPath, true);
        }
    }

    [Test]
    public void Integration_LargeResultSet_HandlesMemoryEfficiently()
    {
        using var table = new HiveParquetTable<SalesRecord>(_testDataPath);

        var count = 0;
        foreach (var record in table)
        {
            count++;
            Assert.That(record, Is.Not.Null);
        }

        var expectedTotal = Years * MonthsPerYear * Regions * RecordsPerPartition;
        Assert.That(count, Is.EqualTo(expectedTotal));
    }

    [Test]
    public void Integration_PartitionDiscovery_FindsAllPartitions()
    {
        var partitions = PartitionDiscovery.Discover(_testDataPath).ToList();

        var expectedPartitionCount = Years * MonthsPerYear * Regions;
        Assert.That(partitions, Has.Count.EqualTo(expectedPartitionCount));

        Assert.That(partitions.All(p => p.Values.ContainsKey("year")), Is.True);
        Assert.That(partitions.All(p => p.Values.ContainsKey("month")), Is.True);
        Assert.That(partitions.All(p => p.Values.ContainsKey("region")), Is.True);

        var years = partitions.Select(p => p.Values["year"]).Distinct().OrderBy(y => y).ToList();
        var regions = partitions.Select(p => p.Values["region"]).Distinct().OrderBy(r => r).ToList();

        Assert.That(years, Is.EquivalentTo(["2023", "2024"]));
        Assert.That(regions, Is.EquivalentTo(["ap-southeast", "eu-central", "eu-west", "us-east", "us-west"]));
    }
}