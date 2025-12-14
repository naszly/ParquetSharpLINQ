using ParquetSharp;
using ParquetSharpLINQ.ParquetSharp;

namespace ParquetSharpLINQ.Tests.Unit;

[TestFixture]
[Category("Unit")]
public class PartitionDiscoveryTests
{
    [SetUp]
    public void Setup()
    {
        _testDirectory = Path.Combine(Path.GetTempPath(), $"ParquetTest_{Guid.NewGuid()}");
        Directory.CreateDirectory(_testDirectory);
    }

    [TearDown]
    public void TearDown()
    {
        if (Directory.Exists(_testDirectory)) Directory.Delete(_testDirectory, true);
    }

    private string _testDirectory = null!;

    [Test]
    public void Discover_WithNullPath_ThrowsArgumentException()
    {
        Assert.Throws<ArgumentException>(() => new FileSystemPartitionDiscovery(null!));
    }

    [Test]
    public void Discover_WithEmptyPath_ThrowsArgumentException()
    {
        Assert.Throws<ArgumentException>(() => new FileSystemPartitionDiscovery(""));
    }

    [Test]
    public void Discover_WithNonExistentDirectory_ThrowsDirectoryNotFoundException()
    {
        Assert.Throws<DirectoryNotFoundException>(() =>
            new FileSystemPartitionDiscovery("/nonexistent/path"));
    }

    [Test]
    public void Discover_WithNoParquetFiles_ReturnsEmpty()
    {
        var discovery = new FileSystemPartitionDiscovery(_testDirectory);
        var partitions = discovery.DiscoverPartitions().ToList();

        Assert.That(partitions, Is.Empty);
    }

    [Test]
    public void Discover_WithRootLevelParquetFile_ReturnsRootPartition()
    {
        var parquetFile = Path.Combine(_testDirectory, "data.parquet");
        File.WriteAllText(parquetFile, "dummy");

        var partitions = new FileSystemPartitionDiscovery(_testDirectory).DiscoverPartitions().ToList();

        Assert.That(partitions, Has.Count.EqualTo(1));
        Assert.That(partitions[0].Path, Is.EqualTo(_testDirectory));
        Assert.That(partitions[0].Values, Is.Empty);
    }

    [Test]
    public void Discover_WithHivePartitions_ParsesPartitionValues()
    {
        var partition1 = Path.Combine(_testDirectory, "year=2023", "region=US");
        Directory.CreateDirectory(partition1);
        File.WriteAllText(Path.Combine(partition1, "data.parquet"), "dummy");

        var partition2 = Path.Combine(_testDirectory, "year=2024", "region=EU");
        Directory.CreateDirectory(partition2);
        File.WriteAllText(Path.Combine(partition2, "data.parquet"), "dummy");

        var partitions = new FileSystemPartitionDiscovery(_testDirectory).DiscoverPartitions().ToList();

        Assert.That(partitions, Has.Count.EqualTo(2));

        var usPartition = partitions.First(p => p.Values.Values.Contains("US"));
        Assert.That(usPartition.Values["year"], Is.EqualTo("2023"));
        Assert.That(usPartition.Values["region"], Is.EqualTo("US"));

        var euPartition = partitions.First(p => p.Values.Values.Contains("EU"));
        Assert.That(euPartition.Values["year"], Is.EqualTo("2024"));
        Assert.That(euPartition.Values["region"], Is.EqualTo("EU"));
    }

    [Test]
    public void Discover_WithNestedPartitions_ParsesAllLevels()
    {
        var partition = Path.Combine(_testDirectory, "year=2024", "month=01", "day=15");
        Directory.CreateDirectory(partition);
        File.WriteAllText(Path.Combine(partition, "data.parquet"), "dummy");

        var partitions = new FileSystemPartitionDiscovery(_testDirectory).DiscoverPartitions().ToList();

        Assert.That(partitions, Has.Count.EqualTo(1));
        Assert.That(partitions[0].Values["year"], Is.EqualTo("2024"));
        Assert.That(partitions[0].Values["month"], Is.EqualTo("01"));
        Assert.That(partitions[0].Values["day"], Is.EqualTo("15"));
    }

    [Test]
    public void Discover_WithMixedPartitionedAndNonPartitionedDirectories_HandlesCorrectly()
    {
        var rootFile = Path.Combine(_testDirectory, "root.parquet");
        File.WriteAllText(rootFile, "dummy");

        var partitioned = Path.Combine(_testDirectory, "year=2024");
        Directory.CreateDirectory(partitioned);
        File.WriteAllText(Path.Combine(partitioned, "data.parquet"), "dummy");

        var nonPartitioned = Path.Combine(_testDirectory, "archive");
        Directory.CreateDirectory(nonPartitioned);
        File.WriteAllText(Path.Combine(nonPartitioned, "old.parquet"), "dummy");

        var partitions = new FileSystemPartitionDiscovery(_testDirectory).DiscoverPartitions().ToList();

        Assert.That(partitions, Has.Count.EqualTo(3));
        Assert.That(partitions.Any(p => p.Values.Count == 0 && p.Path == _testDirectory), Is.True);
        Assert.That(partitions.Any(p => p.Values.ContainsKey("year")), Is.True);
        Assert.That(partitions.Any(p => p.Path.EndsWith("archive")), Is.True);
    }

    [Test]
    public void Discover_WithMultipleFilesInPartition_ReturnsOnePartition()
    {
        var partition = Path.Combine(_testDirectory, "year=2024");
        Directory.CreateDirectory(partition);
        File.WriteAllText(Path.Combine(partition, "file1.parquet"), "dummy");
        File.WriteAllText(Path.Combine(partition, "file2.parquet"), "dummy");
        File.WriteAllText(Path.Combine(partition, "file3.parquet"), "dummy");

        var partitions = new FileSystemPartitionDiscovery(_testDirectory).DiscoverPartitions().ToList();

        Assert.That(partitions, Has.Count.EqualTo(1));
        Assert.That(partitions[0].Values["year"], Is.EqualTo("2024"));
    }

    [Test]
    public void Discover_IgnoresNonParquetFiles()
    {
        var dir = Path.Combine(_testDirectory, "year=2024");
        Directory.CreateDirectory(dir);
        File.WriteAllText(Path.Combine(dir, "file.txt"), "dummy");
        File.WriteAllText(Path.Combine(dir, "file.csv"), "dummy");

        var partitions = new FileSystemPartitionDiscovery(_testDirectory).DiscoverPartitions().ToList();

        Assert.That(partitions, Is.Empty);
    }

    [Test]
    public void Discover_WithInvalidPartitionFormat_TreatsAsNonPartitioned()
    {
        var dir = Path.Combine(_testDirectory, "data", "backup");
        Directory.CreateDirectory(dir);
        File.WriteAllText(Path.Combine(dir, "data.parquet"), "dummy");

        var partitions = new FileSystemPartitionDiscovery(_testDirectory).DiscoverPartitions().ToList();

        Assert.That(partitions, Has.Count.EqualTo(1));
        Assert.That(partitions[0].Values, Is.Empty);
    }

    [Test]
    public void Discover_WithCaseInsensitiveParquetExtension_FindsFiles()
    {
        var dir = Path.Combine(_testDirectory, "year=2024");
        Directory.CreateDirectory(dir);
        File.WriteAllText(Path.Combine(dir, "file.PARQUET"), "dummy");
        File.WriteAllText(Path.Combine(dir, "file.Parquet"), "dummy");

        var partitions = new FileSystemPartitionDiscovery(_testDirectory).DiscoverPartitions().ToList();

        Assert.That(partitions, Has.Count.EqualTo(1));
    }

    [Test]
    public void Discover_WithLongPartitionPaths_HandlesCorrectly()
    {
        var tempDir = Path.Combine(Path.GetTempPath(), $"parquet_test_{Guid.NewGuid():N}");

        try
        {
            Directory.CreateDirectory(tempDir);

            var longPartitionPath = Path.Combine(
                tempDir,
                "event_date=2025-10-01",
                "event_source=very-long-kafka-topic-name-customer-events-engagement-analytics-production-service-identifier",
                "application_name=customer-relationship-management-analytics-data-pipeline-processor-service",
                "environment=production-us-east-1-availability-zone-1a-kubernetes-cluster"
            );

            Directory.CreateDirectory(longPartitionPath);
            File.WriteAllText(Path.Combine(longPartitionPath, "data.parquet"), "dummy");

            var partitions = new FileSystemPartitionDiscovery(tempDir).DiscoverPartitions().ToList();

            Assert.That(partitions, Has.Count.EqualTo(1));
            Assert.That(partitions[0].Values, Contains.Key("event_date"));
            Assert.That(partitions[0].Values["event_date"], Is.EqualTo("2025-10-01"));
            Assert.That(partitions[0].Values, Contains.Key("event_source"));
            Assert.That(partitions[0].Values["event_source"],
                Is.EqualTo(
                    "very-long-kafka-topic-name-customer-events-engagement-analytics-production-service-identifier"));
            Assert.That(partitions[0].Values, Contains.Key("application_name"));
            Assert.That(partitions[0].Values["application_name"],
                Is.EqualTo("customer-relationship-management-analytics-data-pipeline-processor-service"));
            Assert.That(partitions[0].Values, Contains.Key("environment"));
            Assert.That(partitions[0].Values["environment"],
                Is.EqualTo("production-us-east-1-availability-zone-1a-kubernetes-cluster"));
        }
        finally
        {
            if (Directory.Exists(tempDir))
                Directory.Delete(tempDir, true);
        }
    }

    [Test]
    public void Discover_WithVeryLongNestedPartitionPaths_HandlesCorrectly()
    {
        var tempDir = Path.Combine(Path.GetTempPath(), $"parquet_test_{Guid.NewGuid():N}");

        try
        {
            Directory.CreateDirectory(tempDir);

            var nestedPath = Path.Combine(
                tempDir,
                "year=2025",
                "month=10",
                "day=01",
                "hour=14",
                "region=united-states-of-america-east-coast-production-datacenter-availability-zone",
                "data_source=apache-kafka-event-streaming-platform-customer-analytics-topic-partition",
                "application=enterprise-customer-relationship-management-analytics-processing-pipeline",
                "environment=production-highly-available-multi-region-kubernetes-cluster-deployment"
            );

            Directory.CreateDirectory(nestedPath);
            var filePath = Path.Combine(nestedPath, "data.parquet");
            File.WriteAllText(filePath, "dummy");

            Console.WriteLine($"Full path length: {filePath.Length} characters");
            Console.WriteLine($"Path: {filePath}");

            var partitions = new FileSystemPartitionDiscovery(tempDir).DiscoverPartitions().ToList();

            Assert.That(partitions, Has.Count.EqualTo(1));
            Assert.That(partitions[0].Values, Has.Count.EqualTo(8));
            Assert.That(partitions[0].Values["year"], Is.EqualTo("2025"));
            Assert.That(partitions[0].Values["month"], Is.EqualTo("10"));
            Assert.That(partitions[0].Values["day"], Is.EqualTo("01"));
            Assert.That(partitions[0].Values["hour"], Is.EqualTo("14"));
            Assert.That(partitions[0].Values["region"],
                Is.EqualTo("united-states-of-america-east-coast-production-datacenter-availability-zone"));
            Assert.That(partitions[0].Values["data_source"],
                Is.EqualTo("apache-kafka-event-streaming-platform-customer-analytics-topic-partition"));
            Assert.That(partitions[0].Values["application"],
                Is.EqualTo("enterprise-customer-relationship-management-analytics-processing-pipeline"));
            Assert.That(partitions[0].Values["environment"],
                Is.EqualTo("production-highly-available-multi-region-kubernetes-cluster-deployment"));
        }
        finally
        {
            if (Directory.Exists(tempDir))
                Directory.Delete(tempDir, true);
        }
    }

    [Test]
    public void Discover_WithPathNear260CharacterLimit_HandlesCorrectly()
    {
        var tempDir = Path.Combine(Path.GetTempPath(), $"pq_{Guid.NewGuid():N}");

        try
        {
            Directory.CreateDirectory(tempDir);

            var basePathLength = tempDir.Length + "/data.parquet".Length + 1; // +1 for separator
            var availableLength = 250 - basePathLength; // Leave some margin below 260

            var longValue = new string('x', Math.Max(0, availableLength / 2 - 20)); // Split between key and value
            var partitionPath = Path.Combine(tempDir, $"partition={longValue}");

            Directory.CreateDirectory(partitionPath);
            var filePath = Path.Combine(partitionPath, "data.parquet");
            File.WriteAllText(filePath, "dummy");

            Console.WriteLine($"Test path length: {filePath.Length} characters (limit is typically 260)");

            var partitions = new FileSystemPartitionDiscovery(tempDir).DiscoverPartitions().ToList();

            Assert.That(partitions, Has.Count.EqualTo(1));
            Assert.That(partitions[0].Values, Contains.Key("partition"));
            Assert.That(partitions[0].Values["partition"], Has.Length.EqualTo(longValue.Length));
        }
        finally
        {
            if (Directory.Exists(tempDir))
                Directory.Delete(tempDir, true);
        }
    }

    [Test]
    public void ParquetReader_WithLongPartitionPaths_CanReadActualParquetFiles()
    {
        var tempDir = Path.Combine(Path.GetTempPath(), $"pq_long_{Guid.NewGuid():N}");

        try
        {
            Directory.CreateDirectory(tempDir);

            var longPartitionPath = Path.Combine(
                tempDir,
                "event_date=2025-12-07",
                "source=very-long-source-name-to-test-path-handling-in-parquet-reader-with-extended-length",
                "application=customer-engagement-analytics-real-time-processing-pipeline-service",
                "environment=production-multi-region-high-availability-deployment"
            );

            Directory.CreateDirectory(longPartitionPath);
            var parquetFilePath = Path.Combine(longPartitionPath, "data.parquet");

            var schemaColumns = new Column[]
            {
                new Column<long>("id"),
                new Column<string>("name")
            };

            using (var fileWriter = new ParquetFileWriter(parquetFilePath, schemaColumns))
            {
                using var groupWriter = fileWriter.AppendRowGroup();

                using (var idWriter = groupWriter.NextColumn().LogicalWriter<long>())
                {
                    idWriter.WriteBatch(new long[] { 1, 2, 3 });
                }

                using (var nameWriter = groupWriter.NextColumn().LogicalWriter<string>())
                {
                    nameWriter.WriteBatch(new[] { "Alice", "Bob", "Charlie" });
                }
            }

            Console.WriteLine($"Created Parquet file at: {parquetFilePath}");
            Console.WriteLine($"Path length: {parquetFilePath.Length} characters");

            var reader = new ParquetSharpReader();
            var files = reader.ListFiles(longPartitionPath).ToList();

            Assert.That(files, Has.Count.EqualTo(1));
            Assert.That(files[0], Is.EqualTo(parquetFilePath));

            var fileColumns = reader.GetColumns(parquetFilePath).ToList();
            Assert.That(fileColumns, Has.Count.GreaterThan(0));

            using (var fileReader = new ParquetFileReader(parquetFilePath))
            {
                Assert.That(fileReader.FileMetaData.NumRows, Is.EqualTo(3));
                Assert.That(fileReader.FileMetaData.NumRowGroups, Is.EqualTo(1));
                Console.WriteLine($"Successfully opened Parquet file with {parquetFilePath.Length} char path");
            }
        }
        finally
        {
            if (Directory.Exists(tempDir))
                Directory.Delete(tempDir, true);
        }
    }

    [Test]
    public void ParquetReader_WithVeryLongNestedPaths_CanReadActualParquetFiles()
    {
        var tempDir = Path.Combine(Path.GetTempPath(), $"pq_deep_{Guid.NewGuid():N}");

        try
        {
            Directory.CreateDirectory(tempDir);

            var nestedPath = Path.Combine(
                tempDir,
                "year=2025",
                "month=12",
                "day=07",
                "hour=14",
                "region=united-states-production-datacenter-availability-zone-primary",
                "app=enterprise-customer-analytics-real-time-processing-pipeline",
                "env=production-kubernetes-highly-available-multi-region-cluster"
            );

            Directory.CreateDirectory(nestedPath);
            var parquetFilePath = Path.Combine(nestedPath, "events.parquet");

            Console.WriteLine($"Creating Parquet file with path length: {parquetFilePath.Length} chars");

            var schemaColumns = new Column[]
            {
                new Column<long>("id"),
                new Column<long>("value")
            };

            using (var fileWriter = new ParquetFileWriter(parquetFilePath, schemaColumns))
            {
                using var groupWriter = fileWriter.AppendRowGroup();

                using (var idWriter = groupWriter.NextColumn().LogicalWriter<long>())
                {
                    idWriter.WriteBatch(new long[] { 100, 200 });
                }

                using (var valueWriter = groupWriter.NextColumn().LogicalWriter<long>())
                {
                    valueWriter.WriteBatch(new long[] { 12345, 67890 });
                }
            }

            var reader = new ParquetSharpReader();

            var files = reader.ListFiles(nestedPath).ToList();
            Assert.That(files, Has.Count.EqualTo(1));

            var fileColumns = reader.GetColumns(parquetFilePath).ToList();
            Assert.That(fileColumns, Has.Count.EqualTo(2));

            using (var fileReader = new ParquetFileReader(parquetFilePath))
            {
                Assert.That(fileReader.FileMetaData.NumRows, Is.EqualTo(2));
                Assert.That(fileReader.FileMetaData.NumRowGroups, Is.EqualTo(1));
            }

            Console.WriteLine(
                $"Successfully created and opened Parquet file from path with {parquetFilePath.Length} characters");
        }
        finally
        {
            if (Directory.Exists(tempDir))
                Directory.Delete(tempDir, true);
        }
    }
}