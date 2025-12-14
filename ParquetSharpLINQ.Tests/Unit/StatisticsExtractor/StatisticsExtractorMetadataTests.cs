using ParquetSharp;
using ParquetSharpLINQ.Models;

namespace ParquetSharpLINQ.Tests.Unit.StatisticsExtractor;

[TestFixture]
[Category("Unit")]
[Category("StatisticsExtractor")]
public class StatisticsExtractorMetadataTests
{
    private string _testDataDir = null!;
    private ParquetStatisticsExtractor _extractor = null!;

    [OneTimeSetUp]
    public void OneTimeSetUp()
    {
        _testDataDir = Path.Combine(Path.GetTempPath(), $"ParquetStatisticsExtractorTests_{Guid.NewGuid()}");
        Directory.CreateDirectory(_testDataDir);
        _extractor = new ParquetStatisticsExtractor();
    }

    [OneTimeTearDown]
    public void OneTimeTearDown()
    {
        if (Directory.Exists(_testDataDir))
        {
            Directory.Delete(_testDataDir, true);
        }
    }
    
    [Test]
    public void ExtractFromStream_WithMultipleRowGroups_ExtractsAllStatistics()
    {
        // Arrange
        var testFile = StatisticsTestFileHelper.CreateParquetFileWithMultipleRowGroups(_testDataDir);
        using var stream = File.OpenRead(testFile);
        var originalFile = new ParquetFile { Path = testFile };

        // Act
        var enriched = _extractor.ExtractFromStream(stream, originalFile);

        // Assert
        Assert.That(enriched.RowGroups.Count, Is.GreaterThanOrEqualTo(2), 
            "Should have multiple row groups");

        foreach (var rowGroup in enriched.RowGroups)
        {
            Assert.That(rowGroup.ColumnStatisticsByPath, Is.Not.Empty);
            Assert.That(rowGroup.NumRows, Is.GreaterThan(0));
            
            var valueStats = rowGroup.ColumnStatisticsByPath["value"];
            Assert.That(valueStats.HasMinMax, Is.True);
            Assert.That(valueStats.NullCount, Is.Not.Null);
        }
    }

    [Test]
    public void ExtractFromStream_MultipleRowGroups_EachHasCorrectMinMax()
    {
        // Arrange
        var testFile = StatisticsTestFileHelper.CreateParquetFileWithMultipleRowGroups(_testDataDir);
        using var stream = File.OpenRead(testFile);
        var originalFile = new ParquetFile { Path = testFile };

        // Act
        var enriched = _extractor.ExtractFromStream(stream, originalFile);

        // Assert
        Assert.That(enriched.RowGroups, Has.Count.EqualTo(2));
        
        // First row group should have min=1, max=5
        var rg1Stats = enriched.RowGroups[0].ColumnStatisticsByPath["value"];
        var success1 = rg1Stats.TryGetMinMax<int>(out var min1, out var max1);
        Assert.That(success1, Is.True);
        Assert.That(min1, Is.EqualTo(1), "Row group 1 min should be 1");
        Assert.That(max1, Is.EqualTo(5), "Row group 1 max should be 5");
        
        // Second row group should have min=6, max=10
        var rg2Stats = enriched.RowGroups[1].ColumnStatisticsByPath["value"];
        var success2 = rg2Stats.TryGetMinMax<int>(out var min2, out var max2);
        Assert.That(success2, Is.True);
        Assert.That(min2, Is.EqualTo(6), "Row group 2 min should be 6");
        Assert.That(max2, Is.EqualTo(10), "Row group 2 max should be 10");
    }

    [Test]
    public void ExtractFromStream_WithNullValues_TracksNullCount()
    {
        // Arrange
        var testFile = StatisticsTestFileHelper.CreateParquetFileWithNulls(_testDataDir);
        using var stream = File.OpenRead(testFile);
        var originalFile = new ParquetFile { Path = testFile };

        // Act
        var enriched = _extractor.ExtractFromStream(stream, originalFile);

        // Assert
        var stats = enriched.RowGroups[0].ColumnStatisticsByPath;
        Assert.That(stats.ContainsKey("nullable_value"), Is.True);
        var nullableStats = stats["nullable_value"];
        
        Assert.That(nullableStats.NullCount, Is.Not.Null);
        Assert.That(nullableStats.NullCount, Is.GreaterThan(0), 
            "Should track null values");
    }

    [Test]
    public void ExtractFromStream_PopulatesFileMetadata()
    {
        // Arrange
        var testFile = StatisticsTestFileHelper.CreateParquetFileWithIntegers(_testDataDir, [1, 2, 3]);
        using var stream = File.OpenRead(testFile);
        var fileInfo = new FileInfo(testFile);
        var originalFile = new ParquetFile { Path = testFile };

        // Act
        var enriched = _extractor.ExtractFromStream(
            stream, 
            originalFile, 
            fileInfo.Length, 
            fileInfo.LastWriteTimeUtc);

        // Assert
        Assert.That(enriched.Path, Is.EqualTo(testFile));
        Assert.That(enriched.SizeBytes, Is.EqualTo(fileInfo.Length));
        Assert.That(enriched.LastModified, Is.Not.Null);
        Assert.That(enriched.RowCount, Is.EqualTo(3));
    }

    [Test]
    public void ExtractFromStream_WithEmptyFile_HandlesGracefully()
    {
        // Arrange
        var testFile = StatisticsTestFileHelper.CreateEmptyParquetFile(_testDataDir);
        using var stream = File.OpenRead(testFile);
        var originalFile = new ParquetFile { Path = testFile };

        // Act
        var enriched = _extractor.ExtractFromStream(stream, originalFile);

        // Assert
        Assert.That(enriched, Is.Not.Null);
        Assert.That(enriched.RowCount, Is.EqualTo(0));
    }

    [Test]
    public void ExtractFromStream_RowGroupMetadata_IsPopulated()
    {
        // Arrange
        var testFile = StatisticsTestFileHelper.CreateParquetFileWithIntegers(_testDataDir, [1, 2, 3, 4, 5]);
        using var stream = File.OpenRead(testFile);
        var originalFile = new ParquetFile { Path = testFile };

        // Act
        var enriched = _extractor.ExtractFromStream(stream, originalFile);

        // Assert
        var rowGroup = enriched.RowGroups[0];
        Assert.That(rowGroup.Index, Is.EqualTo(0));
        Assert.That(rowGroup.NumRows, Is.EqualTo(5));
        Assert.That(rowGroup.TotalByteSize, Is.GreaterThan(0));
        Assert.That(rowGroup.ColumnStatisticsByPath, Is.Not.Empty);
    }

    [Test]
    public void ExtractFromStream_PhysicalAndLogicalTypes_AreSet()
    {
        // Arrange
        var testFile = StatisticsTestFileHelper.CreateParquetFileWithIntegers(_testDataDir, [1, 2, 3]);
        using var stream = File.OpenRead(testFile);
        var originalFile = new ParquetFile { Path = testFile };

        // Act
        var enriched = _extractor.ExtractFromStream(stream, originalFile);

        // Assert
        var stats = enriched.RowGroups[0].ColumnStatisticsByPath["value"];
        Assert.That(stats.PhysicalType, Is.EqualTo(PhysicalType.Int32));
    }
}

