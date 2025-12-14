using ParquetSharpLINQ.DataGenerator;

namespace ParquetSharpLINQ.Tests.Integration.Helpers;

/// <summary>
/// Common helper for generating test data across integration tests.
/// </summary>
public static class TestDataHelper
{
    public static string CreateTempDirectory(string prefix = "ParquetTest")
    {
        var path = Path.Combine(Path.GetTempPath(), $"{prefix}_{Guid.NewGuid()}");
        Directory.CreateDirectory(path);
        return path;
    }

    public static void GenerateStandardTestData(
        string outputPath,
        int recordsPerPartition = 100,
        int[] years = null!,
        int monthsPerYear = 12)
    {
        years ??= new[] { 2024 };
        
        var generator = new TestDataGenerator();
        generator.GenerateParquetFiles(
            outputPath,
            recordsPerPartition,
            years,
            monthsPerYear
        );
    }

    public static void CleanupDirectory(string path)
    {
        if (Directory.Exists(path))
        {
            try
            {
                Directory.Delete(path, true);
            }
            catch
            {
                // Ignore cleanup errors
            }
        }
    }
}

