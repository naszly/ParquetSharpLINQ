using System.Diagnostics;
using ParquetSharpLINQ.Azure;

namespace ParquetSharpLINQ.Benchmarks;

/// <summary>
/// Performance analysis for Azure Blob Storage
/// </summary>
public static class AzurePerformanceAnalysis
{
    public static void RunAnalysis(string connectionString, string containerName, string prefix = "benchmark_data")
    {
        var startTime = DateTime.Now;

        Console.WriteLine("================================================================================");
        Console.WriteLine("ParquetSharpLINQ Azure Blob Storage Performance Analysis");
        Console.WriteLine($"Started: {startTime:yyyy-MM-dd HH:mm:ss.fff}");
        Console.WriteLine("================================================================================");
        Console.WriteLine();

        Console.WriteLine("Azure Configuration:");
        Console.WriteLine($"  Container: {containerName}");
        Console.WriteLine($"  Prefix: {prefix}");
        Console.WriteLine();

        Console.WriteLine("================================================================================");
        Console.WriteLine("PARTITION PRUNING ANALYSIS (Azure Streaming)");
        Console.WriteLine("================================================================================");
        Console.WriteLine();

        AnalyzePartitionPruning(connectionString, containerName, prefix);

        Console.WriteLine();
        Console.WriteLine("================================================================================");
        Console.WriteLine("CACHING PERFORMANCE ANALYSIS");
        Console.WriteLine("================================================================================");
        Console.WriteLine();

        AnalyzeCachingPerformance(connectionString, containerName, prefix);

        Console.WriteLine();
        Console.WriteLine("================================================================================");
        Console.WriteLine("COLUMN PROJECTION ANALYSIS (Azure)");
        Console.WriteLine("================================================================================");
        Console.WriteLine();

        AnalyzeColumnProjection(connectionString, containerName, prefix);

        var endTime = DateTime.Now;
        var duration = endTime - startTime;

        Console.WriteLine();
        Console.WriteLine("================================================================================");
        Console.WriteLine("ANALYSIS COMPLETE");
        Console.WriteLine($"Finished: {endTime:yyyy-MM-dd HH:mm:ss.fff}");
        Console.WriteLine($"Total Duration: {duration.TotalSeconds:F3}s");
        Console.WriteLine("================================================================================");
    }

    private static void AnalyzePartitionPruning(string connectionString, string containerName, string prefix)
    {
        using var table = new AzureBlobParquetTable<SalesRecord>(connectionString, containerName);

        Console.WriteLine("Note: First query downloads files to memory cache");
        Console.WriteLine();

        // Baseline: Full table scan
        var (fullCount, fullTime) = MeasureQuery(() => table.Count());
        Console.WriteLine("Full table scan (all partitions):");
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

    private static void AnalyzeCachingPerformance(string connectionString, string containerName, string prefix)
    {
        Console.WriteLine("Testing cache performance (same query run multiple times):");
        Console.WriteLine();

        using var table = new AzureBlobParquetTable<SalesRecord>(connectionString, containerName);

        // First run - downloads and caches
        var sw = Stopwatch.StartNew();
        var count1 = table.Where(s => s.Year == 2024 && s.Month == 6).Count();
        var time1 = sw.Elapsed.TotalSeconds;

        Console.WriteLine("First query (downloads to cache):");
        Console.WriteLine($"  Records:  {count1:N0}");
        Console.WriteLine($"  Duration: {time1:F3}s");
        Console.WriteLine();

        // Second run - from cache
        sw.Restart();
        var count2 = table.Where(s => s.Year == 2024 && s.Month == 6).Count();
        var time2 = sw.Elapsed.TotalSeconds;

        Console.WriteLine("Second query (from cache):");
        Console.WriteLine($"  Records:  {count2:N0}");
        Console.WriteLine($"  Duration: {time2:F3}s");
        Console.WriteLine($"  Speedup:  {time1 / time2:F2}x faster");
        Console.WriteLine();

        // Third run - verify consistency
        sw.Restart();
        var count3 = table.Where(s => s.Year == 2024 && s.Month == 6).Count();
        var time3 = sw.Elapsed.TotalSeconds;

        Console.WriteLine("Third query (from cache):");
        Console.WriteLine($"  Records:  {count3:N0}");
        Console.WriteLine($"  Duration: {time3:F3}s");
        Console.WriteLine();

        Console.WriteLine($"Cache efficiency: ~{time1 / time2:F0}x faster after first download");
    }

    private static void AnalyzeColumnProjection(string connectionString, string containerName, string prefix)
    {
        using var table = new AzureBlobParquetTable<SalesRecord>(connectionString, containerName);

        // Full columns
        var (fullCount, fullTime) = MeasureQuery(() =>
            table.Where(s => s.Year == 2024).Take(1000).Count());

        Console.WriteLine("All columns (1000 records):");
        Console.WriteLine($"  Duration: {fullTime:F3}s");
        Console.WriteLine();

        // Select 3 columns
        var (proj3Count, proj3Time) = MeasureQuery(() =>
            table.Where(s => s.Year == 2024)
                .Select(s => new { s.Id, s.ProductName, s.TotalAmount })
                .Take(1000)
                .Count());

        Console.WriteLine("Project 3 columns (Id, ProductName, TotalAmount):");
        Console.WriteLine($"  Duration: {proj3Time:F3}s");
        Console.WriteLine($"  Speedup:  {fullTime / proj3Time:F2}x vs all columns");
        Console.WriteLine();

        // Select 1 column
        var (proj1Count, proj1Time) = MeasureQuery(() =>
            table.Where(s => s.Year == 2024)
                .Select(s => s.Id)
                .Take(1000)
                .Count());

        Console.WriteLine("Project 1 column (Id only):");
        Console.WriteLine($"  Duration: {proj1Time:F3}s");
        Console.WriteLine($"  Speedup:  {fullTime / proj1Time:F2}x vs all columns");
    }

    private static (int Count, double Seconds) MeasureQuery(Func<int> query)
    {
        var sw = Stopwatch.StartNew();
        var count = query();
        sw.Stop();
        return (count, sw.Elapsed.TotalSeconds);
    }
}