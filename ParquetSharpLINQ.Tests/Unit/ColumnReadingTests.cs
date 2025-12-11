using ParquetSharp;
using ParquetSharpLINQ.Attributes;
using NSubstitute;
using ParquetSharpLINQ.ParquetSharp;

namespace ParquetSharpLINQ.Tests.Unit;

[TestFixture]
[Category("Unit")]
public class ColumnReadingTests
{
    [Test]
    public void OnlyEntityMappedColumns_AreRequested_NotAllParquetColumns()
    {
        // Arrange - Create mock reader that tracks which columns are requested
        var testPath = Path.Combine(Path.GetTempPath(), $"ParquetColumnReadTest_{Guid.NewGuid()}");
        Directory.CreateDirectory(testPath);

        try
        {
            // Create a dummy file
            var filePath = Path.Combine(testPath, "data.parquet");
            File.WriteAllText(filePath, "dummy");

            var mockReader = Substitute.For<IParquetReader>();
            
            // Mock returns file with 5 columns (more than entity maps)
            mockReader.GetColumns(Arg.Any<string>()).Returns(new List<Column>
            {
                new(typeof(object), "id"),
                new(typeof(object), "name"),
                new(typeof(object), "age"),
                new(typeof(object), "extra_column_1"),  // Not in entity
                new(typeof(object), "extra_column_2")   // Not in entity
            });

            List<string>? requestedColumns = null;
            mockReader.ReadRows(Arg.Any<string>(), Arg.Any<IEnumerable<string>>()).Returns(callInfo =>
            {
                // Capture which columns were actually requested
                requestedColumns = callInfo.Arg<IEnumerable<string>>().ToList();
                
                // Return mock data
                return new List<ParquetRow>
                {
                    new(["id", "name", "age"], [1L, "Alice", 30]),
                    new(["id", "name", "age"], [2L, "Bob", 25])
                };
            });

            // Act
            using var table = new ParquetTable<PartialEntity>(testPath, mockReader);
            var results = table.ToList();

            // Assert - Verify only the 3 mapped columns were requested, not all 5
            Assert.That(requestedColumns, Is.Not.Null, "ReadRows should have been called");
            Assert.That(requestedColumns, Has.Count.EqualTo(3), 
                "Should only request the 3 mapped columns");
            Assert.That(requestedColumns, Does.Contain("id"));
            Assert.That(requestedColumns, Does.Contain("name"));
            Assert.That(requestedColumns, Does.Contain("age"));
            Assert.That(requestedColumns, Does.Not.Contain("extra_column_1"), 
                "Should NOT request unmapped extra_column_1");
            Assert.That(requestedColumns, Does.Not.Contain("extra_column_2"), 
                "Should NOT request unmapped extra_column_2");
            
            // Also verify the data is correct
            Assert.That(results, Has.Count.EqualTo(2));
            Assert.That(results[0].Name, Is.EqualTo("Alice"));
        }
        finally
        {
            if (Directory.Exists(testPath))
                Directory.Delete(testPath, true);
        }
    }

    [Test]
    public void WithSelectProjection_OnlyRequestedColumns_AreRequested()
    {
        // Arrange
        var testPath = Path.Combine(Path.GetTempPath(), $"ParquetColumnSelectTest_{Guid.NewGuid()}");
        Directory.CreateDirectory(testPath);

        try
        {
            var filePath = Path.Combine(testPath, "data.parquet");
            File.WriteAllText(filePath, "dummy");

            var mockReader = Substitute.For<IParquetReader>();
            mockReader.GetColumns(Arg.Any<string>()).Returns(new List<Column>
            {
                new(typeof(object), "id"),
                new(typeof(object), "name"),
                new(typeof(object), "age")
            });

            List<string>? requestedColumns = null;
            mockReader.ReadRows(Arg.Any<string>(), Arg.Any<IEnumerable<string>>()).Returns(callInfo =>
            {
                requestedColumns = callInfo.Arg<IEnumerable<string>>().ToList();
                return new List<ParquetRow>
                {
                    new(["id", "name", "age"], [1L, "Alice", 30]),
                    new(["id", "name", "age"], [2L, "Bob", 25])
                };
            });

            // Act - Use SELECT to read only 2 of 3 columns
            using var table = new ParquetTable<PartialEntity>(testPath, mockReader);
            var results = table
                .Select(e => new { e.Id, e.Name })
                .ToList();

            // Assert - Should only request the 2 columns from SELECT, not all 3
            Assert.That(requestedColumns, Is.Not.Null, "ReadRows should have been called");
            Assert.That(requestedColumns, Has.Count.EqualTo(2), 
                "Should only request the 2 columns from SELECT projection");
            Assert.That(requestedColumns, Does.Contain("id"));
            Assert.That(requestedColumns, Does.Contain("name"));
            Assert.That(requestedColumns, Does.Not.Contain("age"), 
                "Should NOT request 'age' since it's not in SELECT");
            
            // Verify data
            Assert.That(results, Has.Count.EqualTo(2));
            Assert.That(results[0].Id, Is.EqualTo(1L));
            Assert.That(results[0].Name, Is.EqualTo("Alice"));
            Assert.That(results[1].Id, Is.EqualTo(2L));
            Assert.That(results[1].Name, Is.EqualTo("Bob"));
        }
        finally
        {
            if (Directory.Exists(testPath))
                Directory.Delete(testPath, true);
        }
    }
}

/// <summary>
/// Entity that only maps some columns from a Parquet file (not all)
/// </summary>
public class PartialEntity
{
    [ParquetColumn("id")]
    public long Id { get; set; }

    [ParquetColumn("name")]
    public string Name { get; set; } = string.Empty;

    [ParquetColumn("age")]
    public int Age { get; set; }
    
    // Note: extra_column_1 and extra_column_2 are NOT mapped here
}

