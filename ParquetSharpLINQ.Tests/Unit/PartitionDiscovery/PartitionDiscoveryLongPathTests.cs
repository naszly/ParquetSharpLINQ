using ParquetSharp;
using ParquetSharpLINQ.ParquetSharp;

namespace ParquetSharpLINQ.Tests.Unit.PartitionDiscovery;

[TestFixture]
[Category("Unit")]
[Category("PartitionDiscovery")]
public class PartitionDiscoveryLongPathTests
{
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

            var basePathLength = tempDir.Length + "/data.parquet".Length + 1;
            var availableLength = 250 - basePathLength;

            var longValue = new string('x', Math.Max(0, availableLength / 2 - 20));
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

