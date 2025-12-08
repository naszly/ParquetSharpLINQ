using System.Diagnostics;
using Azure.Storage.Blobs;
using ParquetSharpLINQ.Azure;
using ParquetSharpLINQ.Benchmarks;
using ParquetSharpLINQ.DataGenerator;

namespace ParquetSharpLINQ.Tests.Integration;

/// <summary>
/// Integration tests for Azure Blob Storage using Azurite emulator.
/// Requires Azurite to be running: docker run -p 10000:10000 mcr.microsoft.com/azure-storage/azurite
/// </summary>
[TestFixture]
[Category("Integration")]
[Category("Azure")]
public class AzureIntegrationTests
{
    [OneTimeSetUp]
    public async Task OneTimeSetup()
    {
        if (!await IsAzuriteRunning())
            Assert.Ignore(
                "Azurite is not running. Start it with: docker run -p 10000:10000 mcr.microsoft.com/azure-storage/azurite");

        _containerName = $"test-{Guid.NewGuid():N}";
        var serviceClient = new BlobServiceClient(AzuriteConnectionString);
        _containerClient = serviceClient.GetBlobContainerClient(_containerName);
        await _containerClient.CreateAsync();

        await UploadTestDataAsync();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
        if (_containerClient != null && await _containerClient.ExistsAsync()) await _containerClient.DeleteAsync();
    }

    private const string AzuriteConnectionString =
        "DefaultEndpointsProtocol=http;" +
        "AccountName=devstoreaccount1;" +
        "AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;" +
        "BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;";

    private string _containerName = null!;
    private BlobContainerClient? _containerClient;
    private const int RecordsPerPartition = 100;
    private const int Years = 2;
    private const int MonthsPerYear = 3;
    private const int Regions = 5;
    
    private BlobContainerClient ContainerClient => 
        _containerClient ?? throw new InvalidOperationException("Container client is not initialized.");

    private static async Task<bool> IsAzuriteRunning()
    {
        try
        {
            var client = new BlobServiceClient(AzuriteConnectionString);
            await client.GetAccountInfoAsync();
            return true;
        }
        catch
        {
            return false;
        }
    }

    private async Task UploadTestDataAsync()
    {
        var tempPath = Path.Combine(Path.GetTempPath(), $"ParquetAzureTest_{Guid.NewGuid()}");

        try
        {
            Directory.CreateDirectory(tempPath);

            var generator = new TestDataGenerator();
            generator.GenerateParquetFiles(
                tempPath,
                RecordsPerPartition,
                [2023, 2024],
                MonthsPerYear
            );

            var files = Directory.GetFiles(tempPath, "*.parquet", SearchOption.AllDirectories);

            foreach (var file in files)
            {
                var relativePath = Path.GetRelativePath(tempPath, file).Replace('\\', '/');
                var blobClient = ContainerClient.GetBlobClient(relativePath);

                await using var stream = File.OpenRead(file);
                await blobClient.UploadAsync(stream, true);
            }
        }
        finally
        {
            if (Directory.Exists(tempPath)) Directory.Delete(tempPath, true);
        }
    }

    [Test]
    public void Azure_FullTableScan_ReturnsAllRecords()
    {
        using var table = new AzureHiveParquetTable<SalesRecord>(AzuriteConnectionString, _containerName);
        var expectedCount = Years * MonthsPerYear * Regions * RecordsPerPartition;

        var results = table.ToList();

        Assert.That(results, Has.Count.EqualTo(expectedCount));
        Assert.That(results.All(r => r.Id > 0), Is.True);
        Assert.That(results.All(r => !string.IsNullOrEmpty(r.ProductName)), Is.True);
    }

    [Test]
    public void Azure_PartitionPruning_ByYear_OnlyReadsMatchingPartitions()
    {
        using var table = new AzureHiveParquetTable<SalesRecord>(AzuriteConnectionString, _containerName);
        var expectedCount = MonthsPerYear * Regions * RecordsPerPartition;

        var results = table.Where(s => s.Year == 2024).ToList();

        Assert.That(results, Has.Count.EqualTo(expectedCount));
        Assert.That(results.All(r => r.Year == 2024), Is.True);
    }

    [Test]
    public void Azure_PartitionPruning_ByRegion_OnlyReadsMatchingPartitions()
    {
        using var table = new AzureHiveParquetTable<SalesRecord>(AzuriteConnectionString, _containerName);
        var expectedCount = Years * MonthsPerYear * RecordsPerPartition;

        var results = table.Where(s => s.Region == "eu-west").ToList();

        Assert.That(results, Has.Count.EqualTo(expectedCount));
        Assert.That(results.All(r => r.Region == "eu-west"), Is.True);
    }

    [Test]
    public void Azure_PartitionPruning_MultipleFilters_OnlyReadsSinglePartition()
    {
        using var table = new AzureHiveParquetTable<SalesRecord>(AzuriteConnectionString, _containerName);
        var expectedCount = RecordsPerPartition;

        var results = table
            .Where(s => s.Year == 2024 && s.Month == 1 && s.Region == "us-east")
            .ToList();

        Assert.That(results, Has.Count.EqualTo(expectedCount));
        Assert.That(results.All(r => r.Year == 2024 && r.Month == 1 && r.Region == "us-east"), Is.True);
    }

    [Test]
    public void Azure_CountWithPredicate_ReturnsCorrectCount()
    {
        using var table = new AzureHiveParquetTable<SalesRecord>(AzuriteConnectionString, _containerName);
        var expectedCount = MonthsPerYear * Regions * RecordsPerPartition;

        var count = table.Count(s => s.Year == 2024);

        Assert.That(count, Is.EqualTo(expectedCount));
    }

    [Test]
    public void Azure_AnyWithPredicate_ReturnsTrue()
    {
        using var table = new AzureHiveParquetTable<SalesRecord>(AzuriteConnectionString, _containerName);

        var hasData = table.Any(s => s.Year == 2024 && s.Region == "eu-west");

        Assert.That(hasData, Is.True);
    }

    [Test]
    public void Azure_FirstOrDefaultWithPredicate_ReturnsRecord()
    {
        using var table = new AzureHiveParquetTable<SalesRecord>(AzuriteConnectionString, _containerName);

        var record = table.FirstOrDefault(s => s.Year == 2024 && s.Month == 2);

        Assert.That(record, Is.Not.Null);
        Assert.That(record!.Year, Is.EqualTo(2024));
        Assert.That(record.Month, Is.EqualTo(2));
    }

    [Test]
    public void Azure_ColumnProjection_OnlyReadsRequestedColumns()
    {
        using var table = new AzureHiveParquetTable<SalesRecord>(AzuriteConnectionString, _containerName);

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
    public void Azure_CachingPerformance_SecondQueryIsFaster()
    {
        using var table = new AzureHiveParquetTable<SalesRecord>(AzuriteConnectionString, _containerName);

        var sw = Stopwatch.StartNew();
        var count1 = table.Count(s => s.Year == 2024 && s.Month == 1);
        var time1 = sw.Elapsed;

        sw.Restart();
        var count2 = table.Count(s => s.Year == 2024 && s.Month == 1);
        var time2 = sw.Elapsed;

        Assert.That(count1, Is.EqualTo(count2));
        Assert.That(time2, Is.LessThan(time1), "Cached query should be faster");
    }

    [Test]
    public void Azure_ComplexQuery_CombinesMultipleOperations()
    {
        using var table = new AzureHiveParquetTable<SalesRecord>(AzuriteConnectionString, _containerName);

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
    public void Azure_MultipleQueries_OnSameTable_WorkIndependently()
    {
        using var table = new AzureHiveParquetTable<SalesRecord>(AzuriteConnectionString, _containerName);

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
    public void Azure_PartitionDiscovery_FindsAllPartitions()
    {
        var partitions = AzurePartitionDiscovery.Discover(ContainerClient).ToList();

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

    [Test]
    public void Azure_NoMatchingPartitions_ReturnsEmpty()
    {
        using var table = new AzureHiveParquetTable<SalesRecord>(AzuriteConnectionString, _containerName);

        var results = table.Where(s => s.Year == 2025).ToList();

        Assert.That(results, Is.Empty);
    }

    [Test]
    public void Azure_StreamingFromBlob_WorksCorrectly()
    {
        using var table = new AzureHiveParquetTable<SalesRecord>(AzuriteConnectionString, _containerName);

        var count = 0;
        foreach (var record in table.Where(s => s.Year == 2024))
        {
            count++;
            Assert.That(record, Is.Not.Null);
        }

        var expectedCount = MonthsPerYear * Regions * RecordsPerPartition;
        Assert.That(count, Is.EqualTo(expectedCount));
    }

    [Test]
    public void Azure_WithBlobContainerClient_WorksCorrectly()
    {
        using var table = new AzureHiveParquetTable<SalesRecord>(ContainerClient);

        var count = table.Count(s => s.Year == 2024);
        var expectedCount = MonthsPerYear * Regions * RecordsPerPartition;

        Assert.That(count, Is.EqualTo(expectedCount));
    }

    [Test]
    public void Azure_WithLinqFilter_OnlyReadsMatchingPartitions()
    {
        using var table = new AzureHiveParquetTable<SalesRecord>(
            AzuriteConnectionString,
            _containerName
        );

        var results = table.Where(s => s.Year == 2024).ToList();
        var expectedCount = MonthsPerYear * Regions * RecordsPerPartition;

        Assert.That(results, Has.Count.EqualTo(expectedCount));
        Assert.That(results.All(r => r.Year == 2024), Is.True);
    }

    [Test]
    public void Azure_Aggregations_CalculateCorrectly()
    {
        using var table = new AzureHiveParquetTable<SalesRecord>(AzuriteConnectionString, _containerName);

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
}