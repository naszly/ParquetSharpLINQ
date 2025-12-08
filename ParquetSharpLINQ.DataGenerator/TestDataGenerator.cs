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

    private readonly Random _random = new(42); // Fixed seed for reproducibility

    /// <summary>
    /// Generate test Parquet files with Hive-style partitioning
    /// </summary>
    /// <param name="outputPath">Root directory for generated files</param>
    /// <param name="recordsPerPartition">Number of records per partition</param>
    /// <param name="years">Years to generate data for</param>
    /// <param name="monthsPerYear">Months per year (1-12)</param>
    public void GenerateParquetFiles(
        string outputPath,
        int recordsPerPartition,
        int[] years,
        int monthsPerYear = 12)
    {
        Console.WriteLine($"Generating test data in: {outputPath}");
        Console.WriteLine($"Records per partition: {recordsPerPartition:N0}");
        Console.WriteLine($"Years: {string.Join(", ", years)}");
        Console.WriteLine($"Months per year: {monthsPerYear}");
        Console.WriteLine($"Regions: {Regions.Length}");

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
                    GeneratePartition(outputPath, year, month, region, recordsPerPartition);

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

    private void GeneratePartition(string outputPath, int year, int month, string region, int recordCount)
    {
        var partitionPath = Path.Combine(
            outputPath,
            $"year={year}",
            $"month={month:D2}",
            $"region={region}"
        );

        Directory.CreateDirectory(partitionPath);

        var filePath = Path.Combine(partitionPath, "data.parquet");

        // Define schema
        var columns = new Column[]
        {
            new Column<long>("id"),
            new Column<string>("product_name"),
            new Column<int>("quantity"),
            new Column<decimal>("unit_price", LogicalType.Decimal(10, 2)),
            new Column<decimal>("total_amount", LogicalType.Decimal(10, 2)),
            new Column<DateTime>("sale_date"),
            new Column<long>("customer_id"),
            new Column<bool>("is_discounted")
        };

        using var fileWriter = new ParquetFileWriter(filePath, columns);
        using var groupWriter = fileWriter.AppendRowGroup();

        // Generate data
        var ids = new long[recordCount];
        var productNames = new string[recordCount];
        var quantities = new int[recordCount];
        var unitPrices = new decimal[recordCount];
        var totalAmounts = new decimal[recordCount];
        var saleDates = new DateTime[recordCount];
        var customerIds = new long[recordCount];
        var isDiscounted = new bool[recordCount];

        var startDate = new DateTime(year, month, 1);
        var daysInMonth = DateTime.DaysInMonth(year, month);

        for (var i = 0; i < recordCount; i++)
        {
            var id = (long)year * 100_000_000 + month * 1_000_000 + Regions.ToList().IndexOf(region) * 100_000 + i;
            ids[i] = id;
            productNames[i] = ProductNames[_random.Next(ProductNames.Length)];
            quantities[i] = _random.Next(1, 100);
            unitPrices[i] = Math.Round((decimal)(_random.NextDouble() * 1000 + 10), 2);
            isDiscounted[i] = _random.Next(100) < 30; // 30% discount rate

            if (isDiscounted[i])
                totalAmounts[i] = Math.Round(quantities[i] * unitPrices[i] * 0.9m, 2); // 10% discount
            else
                totalAmounts[i] = Math.Round(quantities[i] * unitPrices[i], 2);

            saleDates[i] = startDate.AddDays(_random.Next(daysInMonth));
            customerIds[i] = _random.Next(1, 100000);
        }

        // Write columns
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

        using (var writer = groupWriter.NextColumn().LogicalWriter<bool>())
        {
            writer.WriteBatch(isDiscounted);
        }
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