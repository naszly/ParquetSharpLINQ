using ParquetSharp;
using ParquetSharpLINQ.Attributes;
using ParquetSharpLINQ.Interfaces;

namespace ParquetSharpLINQ.Tests.Integration.RangeFilter;

/// <summary>
/// Base class for range filter integration tests.
/// Provides common test cases that work across different storage backends (FileSystem, Azure).
/// </summary>
public abstract class RangeFilterTestsBase
{
    /// <summary>
    /// Gets the set of files that were actually read.
    /// </summary>
    protected abstract IReadOnlySet<string> FilesRead { get; }

    /// <summary>
    /// Creates a ParquetTable for the given entity type.
    /// Implementation varies by storage backend (FileSystem vs Azure).
    /// </summary>
    protected abstract ParquetTable<T> CreateTable<T>() where T : new();


    #region File Pruning Tests

    [Test]
    public void RangeFilter_GreaterThan_SkipsFilesWithLowerValues()
    {
        // Arrange - Create 3 files with different value ranges
        CreateFileWithIntRange("file1.parquet", minValue: 0, maxValue: 49);    // Should SKIP: all values < 50
        CreateFileWithIntRange("file2.parquet", minValue: 50, maxValue: 99);   // Should READ: contains values >= 50
        CreateFileWithIntRange("file3.parquet", minValue: 100, maxValue: 149); // Should READ: contains values >= 50

        using var table = CreateTable<RangeIntEntity>();

        // Act - Query with range filter: Value >= 50
        var results = table.Where(x => x.Value >= 50).ToList();

        // Assert - Verify correct results
        Assert.That(results, Is.Not.Empty, "Should return results from file2 and file3");
        Assert.That(results.All(r => r.Value >= 50), Is.True, "All results should satisfy the filter");
        Assert.That(results.Count, Is.EqualTo(100), "Should have 50 values from file2 + 50 from file3");

        // Assert - Verify file pruning actually happened
        var filesRead = FilesRead;
        Assert.That(filesRead, Does.Not.Contain("file1.parquet"), 
            "file1.parquet should be SKIPPED (max=49 < filter min=50)");
        Assert.That(filesRead, Does.Contain("file2.parquet"), 
            "file2.parquet should be READ (contains values >= 50)");
        Assert.That(filesRead, Does.Contain("file3.parquet"), 
            "file3.parquet should be READ (contains values >= 50)");
        Assert.That(filesRead.Count, Is.EqualTo(2), 
            "Only 2 out of 3 files should be read");
    }

    [Test]
    public void RangeFilter_LessThan_SkipsFilesWithHigherValues()
    {
        // Arrange - Create 3 files with different value ranges
        CreateFileWithIntRange("file1.parquet", minValue: 0, maxValue: 49);    // Should READ: contains values < 100
        CreateFileWithIntRange("file2.parquet", minValue: 50, maxValue: 99);   // Should READ: contains values < 100
        CreateFileWithIntRange("file3.parquet", minValue: 100, maxValue: 149); // Should SKIP: all values >= 100

        using var table = CreateTable<RangeIntEntity>();

        // Act - Query with range filter: Value < 100
        var results = table.Where(x => x.Value < 100).ToList();

        // Assert - Verify correct results
        Assert.That(results, Is.Not.Empty, "Should return results from file1 and file2");
        Assert.That(results.All(r => r.Value < 100), Is.True, "All results should satisfy the filter");
        Assert.That(results.Count, Is.EqualTo(100), "Should have 50 values from file1 + 50 from file2");

        // Assert - Verify file pruning actually happened
        var filesRead = FilesRead;
        Assert.That(filesRead, Does.Contain("file1.parquet"), 
            "file1.parquet should be READ (contains values < 100)");
        Assert.That(filesRead, Does.Contain("file2.parquet"), 
            "file2.parquet should be READ (contains values < 100)");
        Assert.That(filesRead, Does.Not.Contain("file3.parquet"), 
            "file3.parquet should be SKIPPED (min=100 >= filter max=100)");
        Assert.That(filesRead.Count, Is.EqualTo(2), 
            "Only 2 out of 3 files should be read");
    }

    [Test]
    public void RangeFilter_BetweenRange_SkipsFilesOutsideRange()
    {
        // Arrange - Create 5 files with different ranges
        CreateFileWithIntRange("file1.parquet", minValue: 0, maxValue: 24);     // Should SKIP: max < 25
        CreateFileWithIntRange("file2.parquet", minValue: 25, maxValue: 49);    // Should READ: overlaps [25, 75)
        CreateFileWithIntRange("file3.parquet", minValue: 50, maxValue: 74);    // Should READ: overlaps [25, 75)
        CreateFileWithIntRange("file4.parquet", minValue: 75, maxValue: 99);    // Should SKIP: min >= 75
        CreateFileWithIntRange("file5.parquet", minValue: 100, maxValue: 124);  // Should SKIP: min >= 75

        using var table = CreateTable<RangeIntEntity>();

        // Act - Query with range filter: 25 <= Value < 75
        var results = table.Where(x => x.Value >= 25 && x.Value < 75).ToList();

        // Assert - Verify correct results
        Assert.That(results, Is.Not.Empty, "Should return results from file2 and file3");
        Assert.That(results.All(r => r.Value >= 25 && r.Value < 75), Is.True, "All results should be in range [25, 75)");
        Assert.That(results.Count, Is.EqualTo(50), "Should have 25 values from file2 + 25 from file3");

        // Assert - Verify file pruning actually happened  
        var filesRead = FilesRead;
        Assert.That(filesRead, Does.Not.Contain("file1.parquet"), 
            "file1.parquet should be SKIPPED (max=24 < filter min=25)");
        Assert.That(filesRead, Does.Contain("file2.parquet"), 
            "file2.parquet should be READ (overlaps [25, 75))");
        Assert.That(filesRead, Does.Contain("file3.parquet"), 
            "file3.parquet should be READ (overlaps [25, 75))");
        Assert.That(filesRead, Does.Not.Contain("file4.parquet"), 
            "file4.parquet should be SKIPPED (min=75 >= filter max=75)");
        Assert.That(filesRead, Does.Not.Contain("file5.parquet"), 
            "file5.parquet should be SKIPPED (min=100 >= filter max=75)");
        Assert.That(filesRead.Count, Is.EqualTo(2), 
            "Only 2 out of 5 files should be read");
    }

    [Test]
    public void RangeFilter_NoMatchingFiles_ReturnsEmptyAndSkipsAllFiles()
    {
        // Arrange - Create files with ranges that don't match the filter
        CreateFileWithIntRange("file1.parquet", minValue: 0, maxValue: 49);
        CreateFileWithIntRange("file2.parquet", minValue: 50, maxValue: 99);

        using var table = CreateTable<RangeIntEntity>();

        // Act - Query for values that don't exist in any file
        var results = table.Where(x => x.Value >= 200).ToList();

        // Assert - Verify no results
        Assert.That(results, Is.Empty, "Should return no results when range doesn't match any file");

        // Assert - Verify ALL files were skipped (most important test!)
        var filesRead = FilesRead;
        Assert.That(filesRead, Is.Empty, 
            "NO files should be read when filter range doesn't match any file statistics");
    }

    [Test]
    public void RangeFilter_ExactMatch_ReadsOnlyMatchingFile()
    {
        // Arrange - Create files where only one contains the exact value
        CreateFileWithIntRange("file1.parquet", minValue: 0, maxValue: 49);
        CreateFileWithIntRange("file2.parquet", minValue: 50, maxValue: 99);
        CreateFileWithIntRange("file3.parquet", minValue: 100, maxValue: 149);

        using var table = CreateTable<RangeIntEntity>();

        // Act - Query for exact value that only exists in file2
        var results = table.Where(x => x.Value == 75).ToList();

        // Assert
        Assert.That(results, Is.Not.Empty, "Should find the exact value");
        Assert.That(results.All(r => r.Value == 75), Is.True, "Should only return the exact match");
    }

    #endregion

    #region Performance Tests

    [Test]
    public void RangeFilter_100Files_OnlyReads6OutOf100()
    {
        // Arrange - Create 100 files with different ranges
        // file000: [0-99], file001: [100-199], ..., file099: [9900-9999]
        for (int i = 0; i < 100; i++)
        {
            int min = i * 100;
            int max = min + 99;
            CreateFileWithIntRange($"file{i:D3}.parquet", min, max);
        }

        using var table = CreateTable<RangeIntEntity>();

        // Act - Query for values in narrow range: [5000, 5600)
        // This should match: file050 [5000-5099], file051 [5100-5199], ..., file055 [5500-5599]
        // That's 6 files out of 100!
        var results = table.Where(x => x.Value >= 5000 && x.Value < 5600).ToList();

        // Assert - Verify correct results
        Assert.That(results, Is.Not.Empty, "Should find results in the target range");
        Assert.That(results.All(r => r.Value >= 5000 && r.Value < 5600), Is.True);
        Assert.That(results.Count, Is.EqualTo(600), "Should have 100 values from each of 6 files");
        
        // Assert - Verify MASSIVE file pruning (THIS IS THE KEY TEST!)
        var filesRead = FilesRead;
        Assert.That(filesRead.Count, Is.EqualTo(6), 
            "Only 6 out of 100 files should be read - 94% reduction!");
        
        // Verify the correct files were read
        Assert.That(filesRead, Does.Contain("file050.parquet"));
        Assert.That(filesRead, Does.Contain("file051.parquet"));
        Assert.That(filesRead, Does.Contain("file052.parquet"));
        Assert.That(filesRead, Does.Contain("file053.parquet"));
        Assert.That(filesRead, Does.Contain("file054.parquet"));
        Assert.That(filesRead, Does.Contain("file055.parquet"));
        
        // Verify some files were NOT read
        Assert.That(filesRead, Does.Not.Contain("file000.parquet"));
        Assert.That(filesRead, Does.Not.Contain("file049.parquet"));
        Assert.That(filesRead, Does.Not.Contain("file056.parquet"));
        Assert.That(filesRead, Does.Not.Contain("file099.parquet"));
    }

    [Test]
    public void RangeFilter_NoFilter_ReadsAllFiles()
    {
        // Arrange
        CreateFileWithIntRange("file1.parquet", minValue: 0, maxValue: 49);
        CreateFileWithIntRange("file2.parquet", minValue: 50, maxValue: 99);
        CreateFileWithIntRange("file3.parquet", minValue: 100, maxValue: 149);

        using var table = CreateTable<RangeIntEntity>();

        // Act - No filter, should read all files
        var results = table.ToList();

        // Assert - Verify all results
        Assert.That(results, Is.Not.Empty);
        Assert.That(results.Count, Is.EqualTo(150), "Should have all 150 records from 3 files");

        // Assert - Verify ALL files were read (baseline test - no optimization)
        var filesRead = FilesRead;
        Assert.That(filesRead.Count, Is.EqualTo(3), 
            "ALL 3 files should be read when no filter is applied");
        Assert.That(filesRead, Does.Contain("file1.parquet"));
        Assert.That(filesRead, Does.Contain("file2.parquet"));
        Assert.That(filesRead, Does.Contain("file3.parquet"));
    }

    #endregion

    #region Date/DateTime Tests

    [Test]
    public void RangeFilter_DateOnly_SkipsFilesOutsideRange()
    {
        // Arrange - Create 3 files with different date ranges
        var date1 = new DateOnly(2024, 1, 1);   // January
        var date2 = new DateOnly(2024, 6, 1);   // June
        var date3 = new DateOnly(2024, 12, 1);  // December

        CreateFileWithDateOnlyRange("file1.parquet", date1, date1.AddMonths(2)); // Jan-Mar
        CreateFileWithDateOnlyRange("file2.parquet", date2, date2.AddMonths(2)); // Jun-Aug
        CreateFileWithDateOnlyRange("file3.parquet", date3, date3.AddMonths(2)); // Dec-Feb

        using var table = CreateTable<RangeDateOnlyEntity>();

        var cutoffDate = new DateOnly(2024, 7, 1);

        // Act - Query for dates >= July 1st (should only match file2 and file3)
        var results = table.Where(x => x.EventDate >= cutoffDate).ToList();

        // Assert - Verify correct results
        Assert.That(results, Is.Not.Empty, "Should return results from file2 and file3");
        Assert.That(results.All(r => r.EventDate >= cutoffDate), Is.True, "All results should be >= cutoff");

        // Assert - Verify file pruning (THIS TESTS THE EPOCH CONVERSION!)
        var filesRead = FilesRead;
        Assert.That(filesRead, Does.Not.Contain("file1.parquet"),
            "file1.parquet should be SKIPPED (max date Mar < cutoff Jul)");
        Assert.That(filesRead, Does.Contain("file2.parquet"),
            "file2.parquet should be READ (contains dates >= Jul)");
        Assert.That(filesRead, Does.Contain("file3.parquet"),
            "file3.parquet should be READ (contains dates >= Jul)");
        Assert.That(filesRead.Count, Is.EqualTo(2),
            "Only 2 out of 3 files should be read");
    }

    #endregion

    #region Helper Methods

    /// <summary>
    /// Creates a Parquet file with the specified range of integer values.
    /// Implementation varies by storage backend (local file vs Azure blob).
    /// </summary>
    protected abstract void CreateFileWithIntRange(string fileName, int minValue, int maxValue);

    /// <summary>
    /// Creates a Parquet file with the specified range of DateOnly values.
    /// Implementation varies by storage backend (local file vs Azure blob).
    /// </summary>
    protected abstract void CreateFileWithDateOnlyRange(string fileName, DateOnly minDate, DateOnly maxDate);

    #endregion
}

#region Test Entity Classes

public class RangeIntEntity
{
    [ParquetColumn("id")]
    public long Id { get; set; }

    [ParquetColumn("value")]
    public int Value { get; set; }
}

public class RangeDateOnlyEntity
{
    [ParquetColumn("id")]
    public long Id { get; set; }

    [ParquetColumn("event_date")]
    public DateOnly EventDate { get; set; }
}

#endregion

