using ParquetSharp;

namespace ParquetSharpLINQ.Tests.Unit.StatisticsExtractor;

/// <summary>
/// Helper class for creating test Parquet files for statistics extraction tests.
/// </summary>
public static class StatisticsTestFileHelper
{
    public static string CreateParquetFileWithIntegers(string testDataDir, int[] values)
    {
        var path = Path.Combine(testDataDir, $"test_integers_{Guid.NewGuid()}.parquet");
        
        var columns = new Column[] { new Column<int>("value") };
        using var writer = new ParquetFileWriter(path, columns);
        using var rowGroup = writer.AppendRowGroup();
        
        using var colWriter = rowGroup.NextColumn().LogicalWriter<int>();
        colWriter.WriteBatch(values);
        
        return path;
    }

    public static string CreateParquetFileWithLongs(string testDataDir, long[] values)
    {
        var path = Path.Combine(testDataDir, $"test_longs_{Guid.NewGuid()}.parquet");
        
        var columns = new Column[] { new Column<long>("id") };
        using var writer = new ParquetFileWriter(path, columns);
        using var rowGroup = writer.AppendRowGroup();
        
        using var colWriter = rowGroup.NextColumn().LogicalWriter<long>();
        colWriter.WriteBatch(values);
        
        return path;
    }

    public static string CreateParquetFileWithDoubles(string testDataDir, double[] values)
    {
        var path = Path.Combine(testDataDir, $"test_doubles_{Guid.NewGuid()}.parquet");
        
        var columns = new Column[] { new Column<double>("price") };
        using var writer = new ParquetFileWriter(path, columns);
        using var rowGroup = writer.AppendRowGroup();
        
        using var colWriter = rowGroup.NextColumn().LogicalWriter<double>();
        colWriter.WriteBatch(values);
        
        return path;
    }

    public static string CreateParquetFileWithFloats(string testDataDir, float[] values)
    {
        var path = Path.Combine(testDataDir, $"test_floats_{Guid.NewGuid()}.parquet");
        
        var columns = new Column[] { new Column<float>("rate") };
        using var writer = new ParquetFileWriter(path, columns);
        using var rowGroup = writer.AppendRowGroup();
        
        using var colWriter = rowGroup.NextColumn().LogicalWriter<float>();
        colWriter.WriteBatch(values);
        
        return path;
    }

    public static string CreateParquetFileWithStrings(string testDataDir, string[] values)
    {
        var path = Path.Combine(testDataDir, $"test_strings_{Guid.NewGuid()}.parquet");
        
        var columns = new Column[] { new Column<string>("name") };
        using var writer = new ParquetFileWriter(path, columns);
        using var rowGroup = writer.AppendRowGroup();
        
        using var colWriter = rowGroup.NextColumn().LogicalWriter<string>();
        colWriter.WriteBatch(values);
        
        return path;
    }

    public static string CreateParquetFileWithDateOnly(string testDataDir, DateOnly[] values)
    {
        var path = Path.Combine(testDataDir, $"test_dates_{Guid.NewGuid()}.parquet");
        
        var columns = new Column[] { new Column<DateOnly>("event_date", LogicalType.Date()) };
        using var writer = new ParquetFileWriter(path, columns);
        using var rowGroup = writer.AppendRowGroup();
        
        using var colWriter = rowGroup.NextColumn().LogicalWriter<DateOnly>();
        colWriter.WriteBatch(values);
        
        return path;
    }

    public static string CreateParquetFileWithBools(string testDataDir, bool[] values)
    {
        var path = Path.Combine(testDataDir, $"test_bools_{Guid.NewGuid()}.parquet");
        
        var columns = new Column[] { new Column<bool>("flag") };
        using var writer = new ParquetFileWriter(path, columns);
        using var rowGroup = writer.AppendRowGroup();
        
        using var colWriter = rowGroup.NextColumn().LogicalWriter<bool>();
        colWriter.WriteBatch(values);
        
        return path;
    }

    public static string CreateParquetFileWithMultipleRowGroups(string testDataDir)
    {
        var path = Path.Combine(testDataDir, $"test_multirowgroup_{Guid.NewGuid()}.parquet");
        
        var columns = new Column[] { new Column<int>("value") };
        using var writer = new ParquetFileWriter(path, columns);
        
        // First row group
        using (var rowGroup = writer.AppendRowGroup())
        {
            using var colWriter = rowGroup.NextColumn().LogicalWriter<int>();
            colWriter.WriteBatch(new[] { 1, 2, 3, 4, 5 });
        }
        
        // Second row group
        using (var rowGroup = writer.AppendRowGroup())
        {
            using var colWriter = rowGroup.NextColumn().LogicalWriter<int>();
            colWriter.WriteBatch(new[] { 6, 7, 8, 9, 10 });
        }
        
        return path;
    }

    public static string CreateParquetFileWithNulls(string testDataDir)
    {
        var path = Path.Combine(testDataDir, $"test_nulls_{Guid.NewGuid()}.parquet");
        
        var columns = new Column[] { new Column<int?>("nullable_value") };
        using var writer = new ParquetFileWriter(path, columns);
        using var rowGroup = writer.AppendRowGroup();
        
        using var colWriter = rowGroup.NextColumn().LogicalWriter<int?>();
        colWriter.WriteBatch(new int?[] { 1, null, 3, null, 5 });
        
        return path;
    }

    public static string CreateParquetFileWithAllNumericTypes(string testDataDir)
    {
        var path = Path.Combine(testDataDir, $"test_numerics_{Guid.NewGuid()}.parquet");
        
        var columns = new Column[]
        {
            new Column<int>("int_col"),
            new Column<long>("long_col"),
            new Column<float>("float_col"),
            new Column<double>("double_col")
        };
        
        using var writer = new ParquetFileWriter(path, columns);
        using var rowGroup = writer.AppendRowGroup();
        
        using (var colWriter = rowGroup.NextColumn().LogicalWriter<int>())
            colWriter.WriteBatch(new[] { 1, 50, 100 });
        
        using (var colWriter = rowGroup.NextColumn().LogicalWriter<long>())
            colWriter.WriteBatch(new[] { 1000L, 3000L, 5000L });
        
        using (var colWriter = rowGroup.NextColumn().LogicalWriter<float>())
            colWriter.WriteBatch(new[] { 1.5f, 2.5f, 3.5f });
        
        using (var colWriter = rowGroup.NextColumn().LogicalWriter<double>())
            colWriter.WriteBatch(new[] { 10.5, 20.5, 30.5 });
        
        return path;
    }

    public static string CreateEmptyParquetFile(string testDataDir)
    {
        var path = Path.Combine(testDataDir, $"test_empty_{Guid.NewGuid()}.parquet");
        
        var columns = new Column[] { new Column<int>("value") };
        using var writer = new ParquetFileWriter(path, columns);
        // Don't write any row groups
        
        return path;
    }
}

