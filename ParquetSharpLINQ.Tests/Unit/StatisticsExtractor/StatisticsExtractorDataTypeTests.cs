using ParquetSharp;
using ParquetSharpLINQ.Models;

namespace ParquetSharpLINQ.Tests.Unit.StatisticsExtractor;

[TestFixture]
[Category("Unit")]
[Category("StatisticsExtractor")]
public class StatisticsExtractorDataTypeTests
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
    public void ExtractFromStream_WithIntegerColumn_ExtractsMinMaxCorrectly()
    {
        // Arrange
        var testFile = StatisticsTestFileHelper.CreateParquetFileWithIntegers(_testDataDir, [10, 50, 25, 100, 5]);
        using var stream = File.OpenRead(testFile);
        var originalFile = new ParquetFile { Path = testFile };

        // Act
        var enriched = _extractor.ExtractFromStream(stream, originalFile);

        // Assert
        Assert.That(enriched.RowGroups, Has.Count.GreaterThan(0));
        var stats = enriched.RowGroups[0].ColumnStatisticsByPath;
        
        Assert.That(stats.ContainsKey("value"), Is.True);
        var valueStats = stats["value"];
        
        Assert.That(valueStats.HasMinMax, Is.True, "Should have min/max values");
        Assert.That(valueStats.MinRaw, Is.Not.Null);
        Assert.That(valueStats.MaxRaw, Is.Not.Null);
        
        var success = valueStats.TryGetMinMax<int>(out var min, out var max);
        Assert.That(success, Is.True, "Should be able to decode int min/max");
        Assert.That(min, Is.EqualTo(5), "Min should be 5");
        Assert.That(max, Is.EqualTo(100), "Max should be 100");
    }

    [Test]
    public void ExtractFromStream_WithLongColumn_ExtractsMinMaxCorrectly()
    {
        // Arrange
        var testFile = StatisticsTestFileHelper.CreateParquetFileWithLongs(_testDataDir, [1000000000L, 5000000000L, 2500000000L]);
        using var stream = File.OpenRead(testFile);
        var originalFile = new ParquetFile { Path = testFile };

        // Act
        var enriched = _extractor.ExtractFromStream(stream, originalFile);

        // Assert
        var stats = enriched.RowGroups[0].ColumnStatisticsByPath;
        Assert.That(stats.ContainsKey("id"), Is.True);
        var idStats = stats["id"];
        
        Assert.That(idStats.HasMinMax, Is.True);
        
        var success = idStats.TryGetMinMax<long>(out var min, out var max);
        Assert.That(success, Is.True);
        Assert.That(min, Is.EqualTo(1000000000L));
        Assert.That(max, Is.EqualTo(5000000000L));
    }

    [Test]
    public void ExtractFromStream_WithDoubleColumn_ExtractsMinMaxCorrectly()
    {
        // Arrange
        var testFile = StatisticsTestFileHelper.CreateParquetFileWithDoubles(_testDataDir, [1.5, 99.9, 50.0, 0.1]);
        using var stream = File.OpenRead(testFile);
        var originalFile = new ParquetFile { Path = testFile };

        // Act
        var enriched = _extractor.ExtractFromStream(stream, originalFile);

        // Assert
        var stats = enriched.RowGroups[0].ColumnStatisticsByPath;
        Assert.That(stats.ContainsKey("price"), Is.True);
        var priceStats = stats["price"];
        
        Assert.That(priceStats.HasMinMax, Is.True);
        
        var success = priceStats.TryGetMinMax<double>(out var min, out var max);
        Assert.That(success, Is.True);
        Assert.That(min, Is.EqualTo(0.1).Within(0.001));
        Assert.That(max, Is.EqualTo(99.9).Within(0.001));
    }

    [Test]
    public void ExtractFromStream_WithFloatColumn_ExtractsMinMaxCorrectly()
    {
        // Arrange
        var testFile = StatisticsTestFileHelper.CreateParquetFileWithFloats(_testDataDir, [1.5f, 99.9f, 50.0f, 0.1f]);
        using var stream = File.OpenRead(testFile);
        var originalFile = new ParquetFile { Path = testFile };

        // Act
        var enriched = _extractor.ExtractFromStream(stream, originalFile);

        // Assert
        var stats = enriched.RowGroups[0].ColumnStatisticsByPath;
        Assert.That(stats.ContainsKey("rate"), Is.True);
        var rateStats = stats["rate"];
        
        Assert.That(rateStats.HasMinMax, Is.True);
        
        var success = rateStats.TryGetMinMax<float>(out var min, out var max);
        Assert.That(success, Is.True);
        Assert.That(min, Is.EqualTo(0.1f).Within(0.001f));
        Assert.That(max, Is.EqualTo(99.9f).Within(0.001f));
    }

    [Test]
    public void ExtractFromStream_WithStringColumn_ExtractsMinMaxCorrectly()
    {
        // Arrange
        var testFile = StatisticsTestFileHelper.CreateParquetFileWithStrings(_testDataDir, ["apple", "zebra", "banana", "mango"]);
        using var stream = File.OpenRead(testFile);
        var originalFile = new ParquetFile { Path = testFile };

        // Act
        var enriched = _extractor.ExtractFromStream(stream, originalFile);

        // Assert
        var stats = enriched.RowGroups[0].ColumnStatisticsByPath;
        Assert.That(stats.ContainsKey("name"), Is.True);
        var nameStats = stats["name"];
        
        if (nameStats.HasMinMax)
        {
            var success = nameStats.TryGetMinMax<string>(out var min, out var max);
            Assert.That(success, Is.True);
            Assert.That(min, Is.EqualTo("apple"));
            Assert.That(max, Is.EqualTo("zebra"));
        }
        else
        {
            Assert.That(nameStats.PhysicalType, Is.EqualTo(PhysicalType.ByteArray));
        }
    }

    [Test]
    public void ExtractFromStream_WithDateOnlyColumn_ExtractsMinMaxCorrectly()
    {
        // Arrange
        var dates = new[]
        {
            new DateOnly(2024, 1, 15),
            new DateOnly(2024, 12, 31),
            new DateOnly(2024, 6, 1),
            new DateOnly(2024, 3, 10)
        };
        var testFile = StatisticsTestFileHelper.CreateParquetFileWithDateOnly(_testDataDir, dates);
        using var stream = File.OpenRead(testFile);
        var originalFile = new ParquetFile { Path = testFile };

        // Act
        var enriched = _extractor.ExtractFromStream(stream, originalFile);

        // Assert
        var stats = enriched.RowGroups[0].ColumnStatisticsByPath;
        Assert.That(stats.ContainsKey("event_date"), Is.True);
        var dateStats = stats["event_date"];
        
        Assert.That(dateStats.HasMinMax, Is.True);
        Assert.That(dateStats.PhysicalType, Is.EqualTo(PhysicalType.Int32));
        Assert.That(dateStats.LogicalType, Is.TypeOf<DateLogicalType>());
        
        var success = dateStats.TryGetMinMax<DateOnly>(out var min, out var max);
        Assert.That(success, Is.True, "Should decode DateOnly values");
        Assert.That(min, Is.EqualTo(new DateOnly(2024, 1, 15)), "Min date should be 2024-01-15");
        Assert.That(max, Is.EqualTo(new DateOnly(2024, 12, 31)), "Max date should be 2024-12-31");
    }

    [Test]
    public void ExtractFromStream_WithAllNumericTypes_ExtractsAllCorrectly()
    {
        // Arrange
        var testFile = StatisticsTestFileHelper.CreateParquetFileWithAllNumericTypes(_testDataDir);
        using var stream = File.OpenRead(testFile);
        var originalFile = new ParquetFile { Path = testFile };

        // Act
        var enriched = _extractor.ExtractFromStream(stream, originalFile);

        // Assert
        var stats = enriched.RowGroups[0].ColumnStatisticsByPath;
        
        Assert.That(stats.ContainsKey("int_col"), Is.True);
        var intStats = stats["int_col"];
        Assert.That(intStats.HasMinMax, Is.True);
        Assert.That(intStats.TryGetMinMax<int>(out var intMin, out var intMax), Is.True);
        Assert.That(intMin, Is.EqualTo(1));
        Assert.That(intMax, Is.EqualTo(100));
        
        Assert.That(stats.ContainsKey("long_col"), Is.True);
        var longStats = stats["long_col"];
        Assert.That(longStats.HasMinMax, Is.True);
        Assert.That(longStats.TryGetMinMax<long>(out var longMin, out var longMax), Is.True);
        Assert.That(longMin, Is.EqualTo(1000L));
        Assert.That(longMax, Is.EqualTo(5000L));
        
        Assert.That(stats.ContainsKey("float_col"), Is.True);
        var floatStats = stats["float_col"];
        Assert.That(floatStats.HasMinMax, Is.True);
        
        Assert.That(stats.ContainsKey("double_col"), Is.True);
        var doubleStats = stats["double_col"];
        Assert.That(doubleStats.HasMinMax, Is.True);
    }
}

