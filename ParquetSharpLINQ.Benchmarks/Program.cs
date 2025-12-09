using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Running;
using ParquetSharpLINQ.DataGenerator;

namespace ParquetSharpLINQ.Benchmarks;

/// <summary>
///     ParquetSharpLINQ Benchmark Suite
///     Local Files:
///     dotnet run -c Release                          - Run full BenchmarkDotNet suite
///     dotnet run -c Release -- generate [path] [num] - Generate test data
///     dotnet run -c Release -- analyze [path]        - Detailed performance analysis
///     dotnet run -c Release -- cleanup [path]        - Remove test data
///     Azure Blob Storage (uses Azurite by default):
///     dotnet run -c Release -- azure-upload [container] [records]   - Generate and upload (uses Azurite)
///     dotnet run -c Release -- azure-analyze [container]            - Analyze performance (uses Azurite)
///     dotnet run -c Release -- azure-cleanup [container]            - Clean up data (uses Azurite)
///     Example:
///     Local:
///     dotnet run -c Release -- generate ./benchmark_data 5000
///     dotnet run -c Release -- analyze ./benchmark_data
///     dotnet run -c Release -- cleanup ./benchmark_data
///     Azure with Azurite (default):
///     dotnet run -c Release -- azure-upload parquet-bench 5000
///     dotnet run -c Release -- azure-analyze parquet-bench
///     dotnet run -c Release -- azure-cleanup parquet-bench
///     Azure with real connection (set AZURE_STORAGE_CONNECTION_STRING env var):
///     export AZURE_STORAGE_CONNECTION_STRING="DefaultEndpointsProtocol=https;..."
///     dotnet run -c Release -- azure-upload parquet-bench 5000
/// </summary>
internal class Program
{
    private const string AzuriteConnectionString =
        "DefaultEndpointsProtocol=http;" +
        "AccountName=devstoreaccount1;" +
        "AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;" +
        "BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;";

    private static void Main(string[] args)
    {
        if (args.Length > 0 && args[0] == "generate")
        {
            var generator = new TestDataGenerator();
            var outputPath = args.Length > 1 ? args[1] : "./benchmark_data";
            var recordsPerPartition = args.Length > 2 ? int.Parse(args[2]) : 10000;

            generator.GenerateParquetFiles(outputPath, recordsPerPartition, Enumerable.Range(2020, 5).ToArray());
            Console.WriteLine($"\nTest data generated in: {Path.GetFullPath(outputPath)}");
            return;
        }

        if (args.Length > 0 && args[0] == "cleanup")
        {
            var outputPath = args.Length > 1 ? args[1] : "./benchmark_data";
            TestDataGenerator.CleanupTestData(outputPath);
            return;
        }

        if (args.Length > 0 && args[0] == "analyze")
        {
            var outputPath = args.Length > 1 ? args[1] : "./benchmark_data";
            PerformanceAnalysis.RunAnalysis(outputPath);
            return;
        }

        // Azure commands
        if (args.Length > 0 && args[0] == "azure-upload")
        {
            var connectionString = Environment.GetEnvironmentVariable("AZURE_STORAGE_CONNECTION_STRING") ??
                                   AzuriteConnectionString;
            var containerName = args.Length > 1 ? args[1] : "parquet-bench";
            var recordsPerPartition = args.Length > 2 ? int.Parse(args[2]) : 5000;

            Console.WriteLine(
                $"Using: {(connectionString == AzuriteConnectionString ? "Azurite (local)" : "Azure Storage")}");
            Console.WriteLine($"Container: {containerName}");
            Console.WriteLine();

            AzureStorageHelper.GenerateAndUpload(
                connectionString,
                containerName,
                recordsPerPartition,
                new[] { 2023, 2024, 2025 });
            return;
        }

        if (args.Length > 0 && args[0] == "azure-analyze")
        {
            var connectionString = Environment.GetEnvironmentVariable("AZURE_STORAGE_CONNECTION_STRING") ??
                                   AzuriteConnectionString;
            var containerName = args.Length > 1 ? args[1] : "parquet-bench";
            var prefix = args.Length > 2 ? args[2] : "benchmark_data";

            Console.WriteLine(
                $"Using: {(connectionString == AzuriteConnectionString ? "Azurite (local)" : "Azure Storage")}");
            Console.WriteLine();

            AzurePerformanceAnalysis.RunAnalysis(connectionString, containerName, prefix);
            return;
        }

        if (args.Length > 0 && args[0] == "azure-cleanup")
        {
            var connectionString = Environment.GetEnvironmentVariable("AZURE_STORAGE_CONNECTION_STRING") ??
                                   AzuriteConnectionString;
            var containerName = args.Length > 1 ? args[1] : "parquet-bench";
            var prefix = args.Length > 2 ? args[2] : "benchmark_data";

            Console.WriteLine(
                $"Using: {(connectionString == AzuriteConnectionString ? "Azurite (local)" : "Azure Storage")}");
            Console.WriteLine();

            AzureStorageHelper.CleanupAzureData(connectionString, containerName, prefix);
            return;
        }

        // Run benchmarks
        BenchmarkRunner.Run<ParquetQueryBenchmarks>();
    }
}

[MemoryDiagnoser]
[SimpleJob(warmupCount: 3, iterationCount: 5)]
public class ParquetQueryBenchmarks
{
    private const int RecordsPerPartition = 10000;
    private string _dataPath = null!;

    [GlobalSetup]
    public void Setup()
    {
        _dataPath = Path.Combine(Path.GetTempPath(), $"benchmark_data_{Guid.NewGuid():N}");
        Console.WriteLine($"Generating test data in: {_dataPath}");

        var generator = new TestDataGenerator();
        // Generate smaller dataset for benchmarking (3 years, 12 months, 5 regions = 180 partitions)
        generator.GenerateParquetFiles(_dataPath, RecordsPerPartition,
            Enumerable.Range(2020, 5).ToArray());

        Console.WriteLine($"Test data ready: {RecordsPerPartition * 180:N0} total records");
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        if (Directory.Exists(_dataPath)) Directory.Delete(_dataPath, true);
    }

    [Benchmark(Description = "Full table scan (no filters)")]
    public int FullTableScan()
    {
        using var table = new ParquetTable<SalesRecord>(_dataPath);
        return table.Count();
    }

    [Benchmark(Description = "Partition pruning (single partition)")]
    public int SinglePartitionQuery()
    {
        using var table = new ParquetTable<SalesRecord>(_dataPath);
        return table.Count(s => s.Year == 2024 && s.Month == 6 && s.Region == "us-east");
    }

    [Benchmark(Description = "Partition pruning (multiple partitions)")]
    public int MultiplePartitionsQuery()
    {
        using var table = new ParquetTable<SalesRecord>(_dataPath);
        return table.Count(s => s.Year == 2024 && s.Region == "us-east");
    }

    [Benchmark(Description = "Column projection (select few columns)")]
    public int ColumnProjection()
    {
        using var table = new ParquetTable<SalesRecord>(_dataPath);
        return table
            .Select(s => new { s.Id, s.ProductName, s.TotalAmount })
            .Count();
    }

    [Benchmark(Description = "Combined: partition + projection")]
    public int PartitionPruningWithProjection()
    {
        using var table = new ParquetTable<SalesRecord>(_dataPath);
        return table
            .Where(s => s.Year == 2024 && s.Month == 6)
            .Select(s => new { s.Id, s.ProductName })
            .Count();
    }

    [Benchmark(Description = "Filter on data column")]
    public decimal FilterOnDataColumn()
    {
        using var table = new ParquetTable<SalesRecord>(_dataPath);
        return table
            .Where(s => s.TotalAmount > 1000)
            .Sum(s => s.TotalAmount);
    }

    [Benchmark(Description = "Complex query (partition + filter + aggregation)")]
    public decimal ComplexQuery()
    {
        using var table = new ParquetTable<SalesRecord>(_dataPath);
        return table
            .Where(s => s.Year == 2024)
            .Where(s => s.Region == "us-east")
            .Where(s => s.IsDiscounted)
            .Sum(s => s.TotalAmount);
    }

    [Benchmark(Description = "GroupBy aggregation")]
    public int GroupByAggregation()
    {
        using var table = new ParquetTable<SalesRecord>(_dataPath);
        return table
            .Where(s => s.Year == 2024)
            .GroupBy(s => s.Region)
            .Count();
    }

    [Benchmark(Description = "First record (early termination)")]
    public SalesRecord? FirstRecord()
    {
        using var table = new ParquetTable<SalesRecord>(_dataPath);
        return table.FirstOrDefault(s => s.Year == 2024);
    }

    [Benchmark(Description = "Take 100 records")]
    public int Take100()
    {
        using var table = new ParquetTable<SalesRecord>(_dataPath);
        return table
            .Where(s => s.Year == 2024)
            .Take(100)
            .Count();
    }
}

[MemoryDiagnoser]
[SimpleJob(warmupCount: 2, iterationCount: 3)]
public class PartitionDiscoveryBenchmarks
{
    private string _dataPath = null!;

    [Params(10, 50, 180)] public int PartitionCount { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        _dataPath = Path.Combine(Path.GetTempPath(), $"partition_bench_{Guid.NewGuid():N}");

        var generator = new TestDataGenerator();
        var years = PartitionCount / 60; // 60 partitions per year (12 months * 5 regions)
        if (years == 0) years = 1;

        generator.GenerateParquetFiles(_dataPath,
            1000,
            Enumerable.Range(2023, years).ToArray());
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        if (Directory.Exists(_dataPath)) Directory.Delete(_dataPath, true);
    }

    [Benchmark]
    public int DiscoverPartitions()
    {
        using var table = new ParquetTable<SalesRecord>(_dataPath);
        return table.DiscoverPartitions().Count();
    }
}