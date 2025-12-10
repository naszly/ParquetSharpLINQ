using NSubstitute;
using ParquetSharp;
using ParquetSharpLINQ.Attributes;

namespace ParquetSharpLINQ.Tests.Unit;

[TestFixture]
[Category("Unit")]
public class PartitionOnlyQueryTests
{
    [Test]
    public void SelectPartitionColumn_DoesNotReadParquetFiles()
    {
        // Arrange - Create mock reader that tracks if files are read
        var testPath = Path.Combine(Path.GetTempPath(), $"PartitionOnlyTest_{Guid.NewGuid()}");
        Directory.CreateDirectory(testPath);

        try
        {
            // Create partition directories
            var partition1 = Path.Combine(testPath, "event_source=source1");
            var partition2 = Path.Combine(testPath, "event_source=source2");
            var partition3 = Path.Combine(testPath, "event_source=source3");
            Directory.CreateDirectory(partition1);
            Directory.CreateDirectory(partition2);
            Directory.CreateDirectory(partition3);
            
            // Create dummy files
            File.WriteAllText(Path.Combine(partition1, "data.parquet"), "dummy");
            File.WriteAllText(Path.Combine(partition2, "data.parquet"), "dummy");
            File.WriteAllText(Path.Combine(partition3, "data.parquet"), "dummy");

            var mockReader = Substitute.For<IParquetReader>();
            var filesWereRead = false;

            // This should NEVER be called for partition-only queries
            mockReader.GetColumns(Arg.Any<string>()).Returns(callInfo =>
            {
                filesWereRead = true;
                return new List<Column>
                {
                    new(typeof(object), "id"),
                    new(typeof(object), "name")
                };
            });

            mockReader.ReadRows(Arg.Any<string>(), Arg.Any<IEnumerable<string>>()).Returns(callInfo =>
            {
                filesWereRead = true;
                return new List<Dictionary<string, object?>>
                {
                    new() { ["id"] = 1L, ["name"] = "Test" }
                };
            });

            // Act - Query only partition column
            using var table = new ParquetTable<EntityWithPartition>(testPath, mockReader);
            var results = table.Select(e => e.EventSource).Distinct().ToList();

            // Assert - Should return partition values WITHOUT reading any Parquet files
            Assert.That(filesWereRead, Is.False, 
                "Should NOT read any Parquet files when querying only partition columns");
            Assert.That(results, Has.Count.EqualTo(3), 
                "Should return 3 distinct partition values");
            Assert.That(results, Does.Contain("source1"));
            Assert.That(results, Does.Contain("source2"));
            Assert.That(results, Does.Contain("source3"));
        }
        finally
        {
            if (Directory.Exists(testPath))
                Directory.Delete(testPath, true);
        }
    }

    [Test]
    public void SelectMixedColumns_ReadsParquetFiles()
    {
        // Arrange
        var testPath = Path.Combine(Path.GetTempPath(), $"MixedColumnsTest_{Guid.NewGuid()}");
        Directory.CreateDirectory(testPath);

        try
        {
            var partition1 = Path.Combine(testPath, "event_source=source1");
            Directory.CreateDirectory(partition1);
            File.WriteAllText(Path.Combine(partition1, "data.parquet"), "dummy");

            var mockReader = Substitute.For<IParquetReader>();
            var filesWereRead = false;

            mockReader.GetColumns(Arg.Any<string>()).Returns(callInfo =>
            {
                filesWereRead = true;
                return new List<Column>
                {
                    new(typeof(object), "id"),
                    new(typeof(object), "name")
                };
            });

            mockReader.ReadRows(Arg.Any<string>(), Arg.Any<IEnumerable<string>>()).Returns(callInfo =>
            {
                return new List<Dictionary<string, object?>>
                {
                    new() { ["id"] = 1L, ["name"] = "Test" }
                };
            });

            // Act - Query partition column AND regular column
            using var table = new ParquetTable<EntityWithPartition>(testPath, mockReader);
            var results = table.Select(e => new { e.EventSource, e.Id }).ToList();

            // Assert - MUST read Parquet files when querying non-partition columns
            Assert.That(filesWereRead, Is.True, 
                "Should read Parquet files when querying mixed partition and regular columns");
            Assert.That(results, Has.Count.EqualTo(1));
        }
        finally
        {
            if (Directory.Exists(testPath))
                Directory.Delete(testPath, true);
        }
    }

    [Test]
    public void SelectMultiplePartitionColumns_DoesNotReadParquetFiles()
    {
        // Arrange
        var testPath = Path.Combine(Path.GetTempPath(), $"MultiPartitionTest_{Guid.NewGuid()}");
        Directory.CreateDirectory(testPath);

        try
        {
            // Create multi-level partitions
            var partition1 = Path.Combine(testPath, "event_source=source1", "region=us");
            var partition2 = Path.Combine(testPath, "event_source=source1", "region=eu");
            Directory.CreateDirectory(partition1);
            Directory.CreateDirectory(partition2);
            File.WriteAllText(Path.Combine(partition1, "data.parquet"), "dummy");
            File.WriteAllText(Path.Combine(partition2, "data.parquet"), "dummy");

            var mockReader = Substitute.For<IParquetReader>();
            var filesWereRead = false;
            
            mockReader.GetColumns(Arg.Any<string>()).Returns(callInfo =>
            {
                filesWereRead = true;
                return new List<Column> { new(typeof(object), "id") };
            });

            // Act - Query multiple partition columns
            using var table = new ParquetTable<EntityWithMultiplePartitions>(testPath, mockReader);
            var results = table.Select(e => new { e.EventSource, e.Region }).ToList();

            // Assert
            Assert.That(filesWereRead, Is.False, 
                "Should NOT read files when all requested columns are partitions");
            Assert.That(results, Has.Count.EqualTo(2));
        }
        finally
        {
            if (Directory.Exists(testPath))
                Directory.Delete(testPath, true);
        }
    }
}

/// <summary>
/// Entity with partition column for testing
/// </summary>
public class EntityWithPartition
{
    [ParquetColumn("id")]
    public long Id { get; set; }

    [ParquetColumn("name")]
    public string Name { get; set; } = string.Empty;

    [ParquetColumn("event_source", IsPartition = true)]
    public string EventSource { get; set; } = string.Empty;
}

/// <summary>
/// Entity with multiple partition columns
/// </summary>
public class EntityWithMultiplePartitions
{
    [ParquetColumn("id")]
    public long Id { get; set; }

    [ParquetColumn("event_source", IsPartition = true)]
    public string EventSource { get; set; } = string.Empty;

    [ParquetColumn("region", IsPartition = true)]
    public string Region { get; set; } = string.Empty;
}

