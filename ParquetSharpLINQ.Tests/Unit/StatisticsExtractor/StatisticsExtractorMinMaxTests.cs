using ParquetSharp;
using ParquetSharpLINQ.Models;
using ParquetSharpLINQ.Statistics;

namespace ParquetSharpLINQ.Tests.Unit.StatisticsExtractor;

[TestFixture]
[Category("Unit")]
[Category("StatisticsExtractor")]
public class StatisticsExtractorMinMaxTests
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
    public void ExtractFromStream_WithNegativeNumbers_ExtractsMinMaxCorrectly()
    {
        // Arrange
        var testFile = StatisticsTestFileHelper.CreateParquetFileWithIntegers(_testDataDir, [-100, -50, 0, 50, 100]);
        using var stream = File.OpenRead(testFile);
        var originalFile = new ParquetFile { Path = testFile };

        // Act
        var enriched = _extractor.ExtractFromStream(stream, originalFile);

        // Assert
        var stats = enriched.RowGroups[0].ColumnStatisticsByPath["value"];
        var success = stats.TryGetMinMax<int>(out var min, out var max);
        
        Assert.That(success, Is.True);
        Assert.That(min, Is.EqualTo(-100));
        Assert.That(max, Is.EqualTo(100));
    }

    [Test]
    public void ExtractFromStream_WithSingleValue_MinEqualsMax()
    {
        // Arrange
        var testFile = StatisticsTestFileHelper.CreateParquetFileWithIntegers(_testDataDir, [42, 42, 42]);
        using var stream = File.OpenRead(testFile);
        var originalFile = new ParquetFile { Path = testFile };

        // Act
        var enriched = _extractor.ExtractFromStream(stream, originalFile);

        // Assert
        var stats = enriched.RowGroups[0].ColumnStatisticsByPath["value"];
        var success = stats.TryGetMinMax<int>(out var min, out var max);
        
        Assert.That(success, Is.True);
        Assert.That(min, Is.EqualTo(42));
        Assert.That(max, Is.EqualTo(42));
    }

    [Test]
    public void ExtractFromStream_IntegerMinMax_ValuesAreExactlyCorrect()
    {
        // Arrange
        var testFile = StatisticsTestFileHelper.CreateParquetFileWithIntegers(_testDataDir, [17, 42, -5, 999, 0, -100]);
        using var stream = File.OpenRead(testFile);
        var originalFile = new ParquetFile { Path = testFile };

        // Act
        var enriched = _extractor.ExtractFromStream(stream, originalFile);

        // Assert
        var stats = enriched.RowGroups[0].ColumnStatisticsByPath["value"];
        var success = stats.TryGetMinMax<int>(out var min, out var max);
        
        Assert.That(success, Is.True);
        Assert.That(min, Is.EqualTo(-100), "Min should be exactly -100");
        Assert.That(max, Is.EqualTo(999), "Max should be exactly 999");
        
        Assert.That(stats.MinRaw, Is.Not.Null);
        Assert.That(stats.MaxRaw, Is.Not.Null);
        Assert.That(BitConverter.ToInt32(stats.MinRaw!, 0), Is.EqualTo(-100));
        Assert.That(BitConverter.ToInt32(stats.MaxRaw!, 0), Is.EqualTo(999));
    }

    [Test]
    public void ExtractFromStream_LongMinMax_ValuesAreExactlyCorrect()
    {
        // Arrange
        var testFile = StatisticsTestFileHelper.CreateParquetFileWithLongs(_testDataDir, 
            [ 
                long.MinValue + 1000, 
                0L, 
                long.MaxValue - 1000,
                1234567890123456L 
            ]);
        using var stream = File.OpenRead(testFile);
        var originalFile = new ParquetFile { Path = testFile };

        // Act
        var enriched = _extractor.ExtractFromStream(stream, originalFile);

        // Assert
        var stats = enriched.RowGroups[0].ColumnStatisticsByPath["id"];
        var success = stats.TryGetMinMax<long>(out var min, out var max);
        
        Assert.That(success, Is.True);
        Assert.That(min, Is.EqualTo(long.MinValue + 1000), "Min should be exactly long.MinValue + 1000");
        Assert.That(max, Is.EqualTo(long.MaxValue - 1000), "Max should be exactly long.MaxValue - 1000");
    }

    [Test]
    public void ExtractFromStream_DoubleMinMax_ValuesAreExactlyCorrect()
    {
        // Arrange
        var testFile = StatisticsTestFileHelper.CreateParquetFileWithDoubles(_testDataDir, [-123.456, 0.0, 789.012, 3.14159]);
        using var stream = File.OpenRead(testFile);
        var originalFile = new ParquetFile { Path = testFile };

        // Act
        var enriched = _extractor.ExtractFromStream(stream, originalFile);

        // Assert
        var stats = enriched.RowGroups[0].ColumnStatisticsByPath["price"];
        var success = stats.TryGetMinMax<double>(out var min, out var max);
        
        Assert.That(success, Is.True);
        Assert.That(min, Is.EqualTo(-123.456).Within(0.000001), "Min should be exactly -123.456");
        Assert.That(max, Is.EqualTo(789.012).Within(0.000001), "Max should be exactly 789.012");
    }

    [Test]
    public void ExtractFromStream_FloatMinMax_ValuesAreExactlyCorrect()
    {
        // Arrange
        var testFile = StatisticsTestFileHelper.CreateParquetFileWithFloats(_testDataDir, [-10.5f, 20.3f, 0.0f, 15.7f]);
        using var stream = File.OpenRead(testFile);
        var originalFile = new ParquetFile { Path = testFile };

        // Act
        var enriched = _extractor.ExtractFromStream(stream, originalFile);

        // Assert
        var stats = enriched.RowGroups[0].ColumnStatisticsByPath["rate"];
        var success = stats.TryGetMinMax<float>(out var min, out var max);
        
        Assert.That(success, Is.True);
        Assert.That(min, Is.EqualTo(-10.5f).Within(0.001f), "Min should be exactly -10.5");
        Assert.That(max, Is.EqualTo(20.3f).Within(0.001f), "Max should be exactly 20.3");
    }

    [Test]
    public void ExtractFromStream_BoolColumn_HasStatistics()
    {
        // Arrange
        var testFile = StatisticsTestFileHelper.CreateParquetFileWithBools(_testDataDir, [true, false, true, true, false]);
        using var stream = File.OpenRead(testFile);
        var originalFile = new ParquetFile { Path = testFile };

        // Act
        var enriched = _extractor.ExtractFromStream(stream, originalFile);

        // Assert
        var stats = enriched.RowGroups[0].ColumnStatisticsByPath;
        Assert.That(stats.ContainsKey("flag"), Is.True);
        var boolStats = stats["flag"];
        
        Assert.That(boolStats.PhysicalType, Is.EqualTo(PhysicalType.Boolean));
        
        if (boolStats.HasMinMax)
        {
            var success = boolStats.TryGetMinMax<bool>(out var min, out var max);
            Assert.That(success, Is.True);
            Assert.That(min, Is.False, "Min bool should be false");
            Assert.That(max, Is.True, "Max bool should be true");
        }
    }

    [Test]
    public void ExtractFromStream_StringMinMax_LexicographicOrderIsCorrect()
    {
        // Arrange
        var testFile = StatisticsTestFileHelper.CreateParquetFileWithStrings(_testDataDir, ["apple", "zebra", "banana", "aardvark", "zoo"]);
        using var stream = File.OpenRead(testFile);
        var originalFile = new ParquetFile { Path = testFile };

        // Act
        var enriched = _extractor.ExtractFromStream(stream, originalFile);

        // Assert
        var stats = enriched.RowGroups[0].ColumnStatisticsByPath["name"];
        
        if (stats.HasMinMax)
        {
            var success = stats.TryGetMinMax<string>(out var min, out var max);
            Assert.That(success, Is.True);
            Assert.That(min, Is.EqualTo("aardvark"), "Min string should be 'aardvark' (lexicographically first)");
            Assert.That(max, Is.EqualTo("zoo"), "Max string should be 'zoo' (lexicographically last)");
        }
    }

    [Test]
    public void ExtractFromStream_MixedNumericSigns_MinMaxCorrect()
    {
        // Arrange
        var testFile = StatisticsTestFileHelper.CreateParquetFileWithLongs(_testDataDir, [-1000L, 1000L, 0L, -5000L, 5000L]);
        using var stream = File.OpenRead(testFile);
        var originalFile = new ParquetFile { Path = testFile };

        // Act
        var enriched = _extractor.ExtractFromStream(stream, originalFile);

        // Assert
        var stats = enriched.RowGroups[0].ColumnStatisticsByPath["id"];
        var success = stats.TryGetMinMax<long>(out var min, out var max);
        
        Assert.That(success, Is.True);
        Assert.That(min, Is.EqualTo(-5000L), "Min should be -5000");
        Assert.That(max, Is.EqualTo(5000L), "Max should be 5000");
    }
}

