using ParquetSharpLINQ.Models;
using ParquetSharpLINQ.Statistics;
using ParquetSharpLINQ.Tests.Integration.Helpers;

namespace ParquetSharpLINQ.Tests.Integration.FileSystem;

[TestFixture]
[Category("Integration")]
[Category("LocalFiles")]
public class FileSystemStatisticsProviderTests
{
    private string _localTestDataPath = null!;
    private FileSystemParquetStatisticsProvider _provider = null!;

    [OneTimeSetUp]
    public void OneTimeSetup()
    {
        _localTestDataPath = TestDataHelper.CreateTempDirectory("ParquetStatisticsTest");
        TestDataHelper.GenerateStandardTestData(_localTestDataPath, recordsPerPartition: 100, years: [2024], monthsPerYear: 1);
        _provider = new FileSystemParquetStatisticsProvider();
    }

    [OneTimeTearDown]
    public void OneTimeTearDown()
    {
        TestDataHelper.CleanupDirectory(_localTestDataPath);
    }

    [Test]
    public async Task EnrichAsync_EndToEndIntegration()
    {
        var parquetFiles = Directory.GetFiles(_localTestDataPath, "*.parquet", SearchOption.AllDirectories);
        var testFile = parquetFiles.First();
        var file = new ParquetFile { Path = testFile };

        var enrichedFile = await _provider.EnrichAsync(file);

        Assert.That(enrichedFile.Path, Is.EqualTo(testFile));
        Assert.That(enrichedFile.SizeBytes, Is.GreaterThan(0), "Should read file size");
        Assert.That(enrichedFile.RowCount, Is.GreaterThan(0), "Should extract row count");
        Assert.That(enrichedFile.RowGroups, Is.Not.Empty, "Should extract row groups");
        Assert.That(enrichedFile.RowGroups[0].ColumnStatisticsByPath, Is.Not.Empty, "Should extract statistics");
        
        var columnStats = enrichedFile.RowGroups[0].ColumnStatisticsByPath;
        Assert.That(columnStats["id"].HasMinMax, Is.True, "Should have min/max statistics");
    }

    [Test]
    public async Task ConcurrentEnrichment_ThreadSafe()
    {
        var parquetFiles = Directory.GetFiles(_localTestDataPath, "*.parquet", SearchOption.AllDirectories);
        var files = parquetFiles.Take(3).Select(f => new ParquetFile { Path = f }).ToList();

        var tasks = files.Select(f => _provider.EnrichAsync(f)).ToList();
        var enrichedFiles = await Task.WhenAll(tasks);

        Assert.That(enrichedFiles, Has.Length.EqualTo(files.Count));
        foreach (var enrichedFile in enrichedFiles)
        {
            Assert.That(enrichedFile.RowCount, Is.GreaterThan(0));
            Assert.That(enrichedFile.RowGroups, Is.Not.Empty);
        }
    }
}

