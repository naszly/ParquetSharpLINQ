using ParquetSharp;

namespace ParquetSharpLINQ.Tests.Unit.PartitionDiscovery;

/// <summary>
/// Helper class for creating test Parquet files with various data types and structures.
/// </summary>
public static class ParquetTestFileHelper
{
    public static string CreateTestParquetFile(string directory, string filename, int[] values)
    {
        var path = Path.Combine(directory, filename);
        var columns = new Column[] { new Column<int>("value") };
        
        using var writer = new ParquetFileWriter(path, columns);
        using var rowGroup = writer.AppendRowGroup();
        using var colWriter = rowGroup.NextColumn().LogicalWriter<int>();
        colWriter.WriteBatch(values);
        
        return path;
    }

    public static string CreateTestParquetFileWithMultipleColumns(string directory, string filename)
    {
        var path = Path.Combine(directory, filename);
        var columns = new Column[]
        {
            new Column<long>("id"),
            new Column<int>("value"),
            new Column<string>("name")
        };
        
        using var writer = new ParquetFileWriter(path, columns);
        using var rowGroup = writer.AppendRowGroup();
        
        using (var idWriter = rowGroup.NextColumn().LogicalWriter<long>())
            idWriter.WriteBatch(new long[] { 1, 2, 3, 4, 5 });
        
        using (var valueWriter = rowGroup.NextColumn().LogicalWriter<int>())
            valueWriter.WriteBatch(new[] { 10, 20, 30, 40, 50 });
        
        using (var nameWriter = rowGroup.NextColumn().LogicalWriter<string>())
            nameWriter.WriteBatch(new[] { "Alice", "Bob", "Charlie", "David", "Eve" });
        
        return path;
    }

    public static string CreateTestParquetFileWithMultipleRowGroups(string directory, string filename)
    {
        var path = Path.Combine(directory, filename);
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

    public static string CreateTestParquetFileWithIntegers(string directory, string filename, int[] values)
    {
        var path = Path.Combine(directory, filename);
        var columns = new Column[] { new Column<int>("int_value") };
        
        using var writer = new ParquetFileWriter(path, columns);
        using var rowGroup = writer.AppendRowGroup();
        using var colWriter = rowGroup.NextColumn().LogicalWriter<int>();
        colWriter.WriteBatch(values);
        
        return path;
    }

    public static string CreateTestParquetFileWithLongs(string directory, string filename, long[] values)
    {
        var path = Path.Combine(directory, filename);
        var columns = new Column[] { new Column<long>("long_value") };
        
        using var writer = new ParquetFileWriter(path, columns);
        using var rowGroup = writer.AppendRowGroup();
        using var colWriter = rowGroup.NextColumn().LogicalWriter<long>();
        colWriter.WriteBatch(values);
        
        return path;
    }

    public static string CreateTestParquetFileWithStrings(string directory, string filename, string[] values)
    {
        var path = Path.Combine(directory, filename);
        var columns = new Column[] { new Column<string>("string_value") };
        
        using var writer = new ParquetFileWriter(path, columns);
        using var rowGroup = writer.AppendRowGroup();
        using var colWriter = rowGroup.NextColumn().LogicalWriter<string>();
        colWriter.WriteBatch(values);
        
        return path;
    }

    public static string CreateTestParquetFileWithDoubles(string directory, string filename, double[] values)
    {
        var path = Path.Combine(directory, filename);
        var columns = new Column[] { new Column<double>("double_value") };
        
        using var writer = new ParquetFileWriter(path, columns);
        using var rowGroup = writer.AppendRowGroup();
        using var colWriter = rowGroup.NextColumn().LogicalWriter<double>();
        colWriter.WriteBatch(values);
        
        return path;
    }

    public static string CreateTestParquetFileWithDateOnly(string directory, string filename, DateOnly[] values)
    {
        var path = Path.Combine(directory, filename);
        var columns = new Column[] { new Column<DateOnly>("date_value", LogicalType.Date()) };
        
        using var writer = new ParquetFileWriter(path, columns);
        using var rowGroup = writer.AppendRowGroup();
        using var colWriter = rowGroup.NextColumn().LogicalWriter<DateOnly>();
        colWriter.WriteBatch(values);
        
        return path;
    }

    public static void CreateParquetFileWithThreeRowGroups(string path)
    {
        var columns = new Column[] { new Column<int>("value") };
        using var writer = new ParquetFileWriter(path, columns);
        
        // Row group 1: values 1-5
        using (var rowGroup = writer.AppendRowGroup())
        {
            using var colWriter = rowGroup.NextColumn().LogicalWriter<int>();
            colWriter.WriteBatch(new[] { 1, 2, 3, 4, 5 });
        }
        
        // Row group 2: values 10-20
        using (var rowGroup = writer.AppendRowGroup())
        {
            using var colWriter = rowGroup.NextColumn().LogicalWriter<int>();
            colWriter.WriteBatch(new[] { 10, 15, 20 });
        }
        
        // Row group 3: values -10 to 0
        using (var rowGroup = writer.AppendRowGroup())
        {
            using var colWriter = rowGroup.NextColumn().LogicalWriter<int>();
            colWriter.WriteBatch(new[] { -10, -5, 0 });
        }
    }
}

