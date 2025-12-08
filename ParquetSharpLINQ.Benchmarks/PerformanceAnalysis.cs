using System.Diagnostics;

namespace ParquetSharpLINQ.Benchmarks;

/// <summary>
///     Detailed performance analysis tool to verify query optimizations
/// </summary>
public static class PerformanceAnalysis
{
    public static void RunAnalysis(string dataPath)
    {
        var startTime = DateTime.Now;

        Console.WriteLine("================================================================================");
        Console.WriteLine("ParquetSharpLINQ Performance Analysis");
        Console.WriteLine($"Started: {startTime:yyyy-MM-dd HH:mm:ss.fff}");
        Console.WriteLine("================================================================================");
        Console.WriteLine();

        // Setup test data
        Console.WriteLine("Initializing test data...");
        SetupTestData(dataPath);

        Console.WriteLine();
        Console.WriteLine("================================================================================");
        Console.WriteLine("PARTITION PRUNING ANALYSIS");
        Console.WriteLine("================================================================================");
        Console.WriteLine();

        AnalyzePartitionPruning(dataPath);

        Console.WriteLine();
        Console.WriteLine("================================================================================");
        Console.WriteLine("COLUMN PROJECTION ANALYSIS");
        Console.WriteLine("================================================================================");
        Console.WriteLine();

        AnalyzeColumnProjection(dataPath);

        Console.WriteLine();
        Console.WriteLine("================================================================================");
        Console.WriteLine("COMBINED OPTIMIZATION ANALYSIS");
        Console.WriteLine("================================================================================");
        Console.WriteLine();

        AnalyzeCombinedOptimizations(dataPath);

        Console.WriteLine();
        Console.WriteLine("================================================================================");
        Console.WriteLine("REGION-SPECIFIC QUERY ANALYSIS");
        Console.WriteLine("================================================================================");
        Console.WriteLine();

        AnalyzeRegionQueries(dataPath);

        var endTime = DateTime.Now;
        var duration = endTime - startTime;

        Console.WriteLine();
        Console.WriteLine("================================================================================");
        Console.WriteLine("ANALYSIS COMPLETE");
        Console.WriteLine($"Finished: {endTime:yyyy-MM-dd HH:mm:ss.fff}");
        Console.WriteLine($"Total Duration: {duration.TotalSeconds:F3}s");
        Console.WriteLine("================================================================================");
    }

    private static void SetupTestData(string dataPath)
    {
        if (Directory.Exists(dataPath))
        {
            var partitionCount = PartitionDiscovery.Discover(dataPath).Count();
            Console.WriteLine($"  Using existing test data: {dataPath}");
            Console.WriteLine($"  Discovered partitions: {partitionCount}");
            return;
        }

        Console.WriteLine($"  Error: Test data directory not found: {dataPath}");
        Console.WriteLine("  Please generate test data first:");
        Console.WriteLine($"    dotnet run -c Release -- generate {dataPath} 5000");
        Environment.Exit(1);
    }

    private static void AnalyzePartitionPruning(string dataPath)
    {
        using var table = new HiveParquetTable<SalesRecord>(dataPath);

        // Baseline: Full table scan
        var (fullCount, fullTime) = MeasureQuery(() => table.Count());
        Console.WriteLine("Full table scan:");
        Console.WriteLine($"  Records:      {fullCount:N0}");
        Console.WriteLine($"  Duration:     {fullTime:F3}s");
        Console.WriteLine($"  Throughput:   {fullCount / fullTime:N0} records/sec");
        Console.WriteLine();

        // Single partition: year + month + region
        var (singleCount, singleTime) = MeasureQuery(() =>
            table.Where(s => s.Year == 2024 && s.Month == 6 && s.Region == "eu-west").Count());

        Console.WriteLine("Single partition filter (year=2024 AND month=6 AND region=eu-west):");
        Console.WriteLine($"  Records:      {singleCount:N0}");
        Console.WriteLine($"  Duration:     {singleTime:F3}s");
        Console.WriteLine($"  Throughput:   {singleCount / singleTime:N0} records/sec");
        Console.WriteLine($"  Speedup:      {fullTime / singleTime:F2}x vs full scan");
        Console.WriteLine($"  Data ratio:   {(double)singleCount / fullCount:P2} of total");
        Console.WriteLine();

        // Region filter only
        var (regionCount, regionTime) = MeasureQuery(() =>
            table.Where(s => s.Region == "eu-west").Count());

        Console.WriteLine("Region filter (region=eu-west):");
        Console.WriteLine($"  Records:      {regionCount:N0}");
        Console.WriteLine($"  Duration:     {regionTime:F3}s");
        Console.WriteLine($"  Throughput:   {regionCount / regionTime:N0} records/sec");
        Console.WriteLine($"  Speedup:      {fullTime / regionTime:F2}x vs full scan");
        Console.WriteLine($"  Data ratio:   {(double)regionCount / fullCount:P2} of total");
        Console.WriteLine();

        // Year + Region
        var (yearRegionCount, yearRegionTime) = MeasureQuery(() =>
            table.Where(s => s.Year == 2024 && s.Region == "us-east").Count());

        Console.WriteLine("Combined filter (year=2024 AND region=us-east):");
        Console.WriteLine($"  Records:      {yearRegionCount:N0}");
        Console.WriteLine($"  Duration:     {yearRegionTime:F3}s");
        Console.WriteLine($"  Throughput:   {yearRegionCount / yearRegionTime:N0} records/sec");
        Console.WriteLine($"  Speedup:      {fullTime / yearRegionTime:F2}x vs full scan");
        Console.WriteLine($"  Data ratio:   {(double)yearRegionCount / fullCount:P2} of total");
    }

    private static void AnalyzeColumnProjection(string dataPath)
    {
        using var table = new HiveParquetTable<SalesRecord>(dataPath);

        // Full columns (limited to 1000 records for fair comparison)
        var (fullCount, fullTime) = MeasureQuery(() => table.Take(1000).Count());
        Console.WriteLine("All columns (1000 records):");
        Console.WriteLine($"  Duration:     {fullTime:F3}s");
        Console.WriteLine();

        // Select 3 columns
        var (proj3Count, proj3Time) = MeasureQuery(() =>
            table.Select(s => new { s.Id, s.ProductName, s.TotalAmount }).Take(1000).Count());

        Console.WriteLine("Project 3 columns (Id, ProductName, TotalAmount):");
        Console.WriteLine($"  Duration:     {proj3Time:F3}s");
        Console.WriteLine($"  Speedup:      {fullTime / proj3Time:F2}x vs all columns");
        Console.WriteLine();

        // Select 1 column
        var (proj1Count, proj1Time) = MeasureQuery(() =>
            table.Select(s => s.TotalAmount).Take(1000).Count());

        Console.WriteLine("Project 1 column (TotalAmount):");
        Console.WriteLine($"  Duration:     {proj1Time:F3}s");
        Console.WriteLine($"  Speedup:      {fullTime / proj1Time:F2}x vs all columns");
    }

    private static void AnalyzeCombinedOptimizations(string dataPath)
    {
        using var table = new HiveParquetTable<SalesRecord>(dataPath);

        // Baseline: Full scan with all columns
        var (baselineCount, baselineTime) = MeasureQuery(() => table.Count());
        Console.WriteLine("Baseline (full scan, all columns):");
        Console.WriteLine($"  Records:      {baselineCount:N0}");
        Console.WriteLine($"  Duration:     {baselineTime:F3}s");
        Console.WriteLine();

        // Partition pruning only
        var (partitionCount, partitionTime) = MeasureQuery(() =>
            table.Where(s => s.Region == "eu-west").Count());

        Console.WriteLine("Partition pruning only (region=eu-west):");
        Console.WriteLine($"  Records:      {partitionCount:N0}");
        Console.WriteLine($"  Duration:     {partitionTime:F3}s");
        Console.WriteLine($"  Speedup:      {baselineTime / partitionTime:F2}x vs baseline");
        Console.WriteLine();

        // Partition + projection
        var (combinedCount, combinedTime) = MeasureQuery(() =>
            table.Where(s => s.Region == "eu-west")
                .Select(s => new { s.Id, s.ProductName, s.TotalAmount })
                .Count());

        Console.WriteLine("Partition + projection (region=eu-west, 3 columns):");
        Console.WriteLine($"  Records:      {combinedCount:N0}");
        Console.WriteLine($"  Duration:     {combinedTime:F3}s");
        Console.WriteLine($"  Speedup:      {baselineTime / combinedTime:F2}x vs baseline");
        Console.WriteLine($"  Speedup:      {partitionTime / combinedTime:F2}x vs partition only");
    }

    private static void AnalyzeRegionQueries(string dataPath)
    {
        using var table = new HiveParquetTable<SalesRecord>(dataPath);

        var regions = new[] { "us-east", "us-west", "eu-central", "eu-west", "ap-southeast" };

        Console.WriteLine("Individual region query performance:");
        Console.WriteLine();

        foreach (var region in regions)
        {
            var (count, time) = MeasureQuery(() =>
                table.Where(s => s.Region == region).Count());

            Console.WriteLine(
                $"  region={region,-15}  Records: {count,10:N0}  Duration: {time,8:F3}s  Throughput: {count / time,10:N0} rec/s");
        }

        Console.WriteLine();

        // Multi-region OR query
        var (multiCount, multiTime) = MeasureQuery(() =>
            table.Where(s => s.Region == "eu-west" || s.Region == "eu-central").Count());

        Console.WriteLine("Multi-region query (eu-west OR eu-central):");
        Console.WriteLine($"  Records:      {multiCount:N0}");
        Console.WriteLine($"  Duration:     {multiTime:F3}s");
        Console.WriteLine($"  Throughput:   {multiCount / multiTime:N0} records/sec");
    }

    private static (int Count, double TimeSeconds) MeasureQuery(Func<int> query)
    {
        // Warmup
        query();

        var sw = Stopwatch.StartNew();
        var count = query();
        sw.Stop();

        return (count, sw.Elapsed.TotalSeconds);
    }
}