using ParquetSharpLINQ.Tests.Integration.Delta;

namespace ParquetSharpLINQ.Tests.Integration.FileSystem;

[TestFixture]
[Category("Integration")]
public class FileSystemDeltaLakeTests : DeltaLakeTestsBase
{
    private static readonly string TestDataPath = Path.Combine(
        TestContext.CurrentContext.TestDirectory,
        "..", "..", "..", "..",
        "ParquetSharpLINQ.Tests", "Integration", "Delta", "delta_test_data");

    [OneTimeSetUp]
    public void OneTimeSetUp()
    {
        if (!Directory.Exists(TestDataPath))
        {
            Assert.Inconclusive(
                $"Delta test data not found at {TestDataPath}. " +
                "Run 'python3 Integration/Delta/generate_delta_test_data.py' to generate test data.");
        }
    }

    protected override ParquetTable<T> CreateTable<T>(string tableName)
    {
        var tablePath = Path.Combine(TestDataPath, tableName);
        
        if (!Directory.Exists(tablePath))
        {
            Assert.Inconclusive($"Delta table '{tableName}' not found. Generate test data first.");
        }

        return ParquetTable<T>.Factory.FromFileSystem(tablePath);
    }

    [Test]
    public void DeltaLog_ExistsAndContainsJsonFiles()
    {
        var tablePath = Path.Combine(TestDataPath, "simple_delta");
        var deltaLogPath = Path.Combine(tablePath, "_delta_log");
        
        Assert.That(Directory.Exists(deltaLogPath), Is.True, 
            "Delta log directory should exist");

        var jsonFiles = Directory.GetFiles(deltaLogPath, "*.json");
        
        Assert.That(jsonFiles, Is.Not.Empty, 
            "Delta log should contain JSON transaction log files");
    }
}
