using System.Numerics;
using ParquetSharp;

namespace ParquetSharpLINQ.DataGenerator;

/// <summary>
/// Generates test Parquet files for performance benchmarking
/// </summary>
public class TestDataGenerator
{
    private static readonly string[] ProductNames =
    [
        "Laptop", "Desktop", "Monitor", "Keyboard", "Mouse",
        "Headphones", "Webcam", "Microphone", "Speaker", "Tablet"
    ];

    private static readonly string[] Regions =
    [
        "us-east", "us-west", "eu-central", "eu-west", "ap-southeast"
    ];

    /// <summary>
    /// Generate test Parquet files with Hive-style partitioning
    /// </summary>
    /// <param name="outputPath">Root directory for generated files</param>
    /// <param name="recordsPerPartition">Number of records per partition</param>
    /// <param name="years">Years to generate data for</param>
    /// <param name="monthsPerYear">Months per year (1-12)</param>
    /// <param name="rowGroupsPerFile">Number of row groups per file</param>
    /// <param name="filesPerPartition">Number of files per partition</param>
    public void GenerateParquetFiles(
        string outputPath,
        int recordsPerPartition,
        int[] years,
        int monthsPerYear = 12,
        int rowGroupsPerFile = 1,
        int filesPerPartition = 1)
    {
        Console.WriteLine($"Generating test data in: {outputPath}");
        Console.WriteLine($"Records per partition: {recordsPerPartition:N0}");
        Console.WriteLine($"Years: {string.Join(", ", years)}");
        Console.WriteLine($"Months per year: {monthsPerYear}");
        Console.WriteLine($"Regions: {Regions.Length}");
        Console.WriteLine($"Row groups per file: {rowGroupsPerFile}");
        Console.WriteLine($"Files per partition: {filesPerPartition}");

        var totalPartitions = years.Length * monthsPerYear * Regions.Length;
        var totalRecords = totalPartitions * recordsPerPartition;

        Console.WriteLine($"Total partitions: {totalPartitions}");
        Console.WriteLine($"Total records: {totalRecords:N0}");
        Console.WriteLine();

        var partitionCount = 0;
        var startTime = DateTime.Now;

        foreach (var year in years)
            for (var month = 1; month <= monthsPerYear; month++)
                foreach (var region in Regions)
                {
                    partitionCount++;
                    GeneratePartition(outputPath, year, month, region, recordsPerPartition, rowGroupsPerFile, filesPerPartition);

                    if (partitionCount % 10 == 0)
                    {
                        var elapsed = DateTime.Now - startTime;
                        var recordsGenerated = partitionCount * recordsPerPartition;
                        var recordsPerSecond = recordsGenerated / elapsed.TotalSeconds;
                        Console.WriteLine($"Progress: {partitionCount}/{totalPartitions} partitions " +
                                          $"({recordsGenerated:N0} records, {recordsPerSecond:N0} rec/sec)");
                    }
                }

        var totalElapsed = DateTime.Now - startTime;
        Console.WriteLine();
        Console.WriteLine("Generation complete!");
        Console.WriteLine($"Total time: {totalElapsed.TotalSeconds:F2}s");
        Console.WriteLine($"Records/second: {totalRecords / totalElapsed.TotalSeconds:N0}");
    }

    private void GeneratePartition(
        string outputPath,
        int year,
        int month,
        string region,
        int partitionRecordCount,
        int rowGroupsPerFile,
        int filesPerPartition)
    {
        var partitionPath = Path.Combine(
            outputPath,
            $"year={year}",
            $"month={month:D2}",
            $"region={region}"
        );

        Directory.CreateDirectory(partitionPath);

        if (rowGroupsPerFile < 1)
            throw new ArgumentOutOfRangeException(nameof(rowGroupsPerFile), "Row group count must be at least 1.");
        if (filesPerPartition < 1)
            throw new ArgumentOutOfRangeException(nameof(filesPerPartition), "File count must be at least 1.");

        var baseRecordsPerFile = partitionRecordCount / filesPerPartition;
        var fileRemainder = partitionRecordCount % filesPerPartition;

        var startDate = new DateTime(year, month, 1);
        var daysInMonth = DateTime.DaysInMonth(year, month);
        var regionIndex = Array.IndexOf(Regions, region);

        var globalIndex = 0;
        for (var fileIndex = 0; fileIndex < filesPerPartition; fileIndex++)
        {
            var recordsInFile = baseRecordsPerFile + (fileIndex < fileRemainder ? 1 : 0);
            if (recordsInFile == 0)
                continue;

            var fileName = filesPerPartition == 1 ? "data.parquet" : $"data_{fileIndex:D3}.parquet";
            var filePath = Path.Combine(partitionPath, fileName);
            globalIndex = GenerateFile(
                filePath,
                rowGroupsPerFile,
                recordsInFile,
                partitionRecordCount,
                globalIndex,
                year,
                month,
                regionIndex,
                startDate,
                daysInMonth);
        }
    }

    private static int GenerateFile(
        string filePath,
        int rowGroupsPerFile,
        int recordsInFile,
        int partitionRecordCount,
        int globalIndex,
        int year,
        int month,
        int regionIndex,
        DateTime startDate,
        int daysInMonth)
    {
        var columns = new Column[]
        {
            new Column<long>("id"),
            new Column<string>("product_name"),
            new Column<int>("quantity"),
            new Column<decimal>("unit_price", LogicalType.Decimal(10, 2)),
            new Column<decimal>("total_amount", LogicalType.Decimal(10, 2)),
            new Column<DateTime>("sale_date"),
            new Column<long>("customer_id"),
            new Column<string>("client_id"),
            new Column<bool>("is_discounted")
        };

        using var fileWriter = new ParquetFileWriter(filePath, columns);

        var baseRecordsPerRowGroup = recordsInFile / rowGroupsPerFile;
        var remainder = recordsInFile % rowGroupsPerFile;

        for (var rowGroupIndex = 0; rowGroupIndex < rowGroupsPerFile; rowGroupIndex++)
        {
            var recordsInRowGroup = baseRecordsPerRowGroup + (rowGroupIndex < remainder ? 1 : 0);
            if (recordsInRowGroup == 0)
                continue;

            using var groupWriter = fileWriter.AppendRowGroup();

            var ids = new long[recordsInRowGroup];
            var productNames = new string[recordsInRowGroup];
            var quantities = new int[recordsInRowGroup];
            var unitPrices = new decimal[recordsInRowGroup];
            var totalAmounts = new decimal[recordsInRowGroup];
            var saleDates = new DateTime[recordsInRowGroup];
            var customerIds = new long[recordsInRowGroup];
            var clientIds = new string[recordsInRowGroup];
            var isDiscounted = new bool[recordsInRowGroup];

            for (var i = 0; i < recordsInRowGroup; i++, globalIndex++)
            {
                var id = (long)year * 100_000_000 + month * 1_000_000 +
                         regionIndex * 100_000 + globalIndex;
                ids[i] = id;
                productNames[i] = ProductNames[(globalIndex + rowGroupIndex) % ProductNames.Length];
                quantities[i] = (globalIndex % 100) + 1;
                unitPrices[i] = 10m + (globalIndex % 1000) / 10m;
                isDiscounted[i] = globalIndex % 2 == 0;

                if (isDiscounted[i])
                    totalAmounts[i] = Math.Round(quantities[i] * unitPrices[i] * 0.9m, 2); // 10% discount
                else
                    totalAmounts[i] = Math.Round(quantities[i] * unitPrices[i], 2);

                saleDates[i] = startDate.AddDays(globalIndex % daysInMonth);
                customerIds[i] = (globalIndex % 100000) + 1;
                clientIds[i] = CreateGuidString(globalIndex, partitionRecordCount);
            }

            using (var writer = groupWriter.NextColumn().LogicalWriter<long>())
            {
                writer.WriteBatch(ids);
            }

            using (var writer = groupWriter.NextColumn().LogicalWriter<string>())
            {
                writer.WriteBatch(productNames);
            }

            using (var writer = groupWriter.NextColumn().LogicalWriter<int>())
            {
                writer.WriteBatch(quantities);
            }

            using (var writer = groupWriter.NextColumn().LogicalWriter<decimal>())
            {
                writer.WriteBatch(unitPrices);
            }

            using (var writer = groupWriter.NextColumn().LogicalWriter<decimal>())
            {
                writer.WriteBatch(totalAmounts);
            }

            using (var writer = groupWriter.NextColumn().LogicalWriter<DateTime>())
            {
                writer.WriteBatch(saleDates);
            }

            using (var writer = groupWriter.NextColumn().LogicalWriter<long>())
            {
                writer.WriteBatch(customerIds);
            }

            using (var writer = groupWriter.NextColumn().LogicalWriter<string>())
            {
                writer.WriteBatch(clientIds);
            }

            using (var writer = groupWriter.NextColumn().LogicalWriter<bool>())
            {
                writer.WriteBatch(isDiscounted);
            }
        }

        return globalIndex;
    }

    private static string CreateGuidString(int index, int totalCount)
    {
        if (totalCount <= 1)
            return "00000000000000000000000000000000";

        var max = (BigInteger.One << 128) - 1;
        var value = max * index / (totalCount - 1);
        var high = (ulong)(value >> 64);
        var low = (ulong)(value & ((BigInteger.One << 64) - 1));
        var hex = $"{high:x16}{low:x16}"; // 32 hex chars
        // Format as standard GUID: 8-4-4-4-12
        return hex.Substring(0, 8) + "-" +
               hex.Substring(8, 4) + "-" +
               hex.Substring(12, 4) + "-" +
               hex.Substring(16, 4) + "-" +
               hex.Substring(20, 12);
    }

    /// <summary>
    /// Clean up generated test data
    /// </summary>
    public static void CleanupTestData(string outputPath)
    {
        if (Directory.Exists(outputPath))
        {
            Console.WriteLine($"Cleaning up: {outputPath}");
            Directory.Delete(outputPath, true);
            Console.WriteLine("Cleanup complete");
        }
    }
}
