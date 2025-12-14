using ParquetSharp;
using ParquetSharpLINQ.Attributes;

namespace ParquetSharpLINQ.Tests.Unit;

[TestFixture]
[Category("Unit")]
public class TypeMismatchTests
{
    [Test]
    public void ByteProperty_WithValueOver255_ThrowsOverflowException()
    {
        var tempDir = Path.Combine(Path.GetTempPath(), $"ParquetTypeMismatch_{Guid.NewGuid()}");
        Directory.CreateDirectory(tempDir);

        try
        {
            var filePath = Path.Combine(tempDir, "overflow.parquet");
            
            using (var fileWriter = new ParquetFileWriter(filePath, [new Column<int>("value")]))
            {
                using var rowGroup = fileWriter.AppendRowGroup();
                using var valueWriter = rowGroup.NextColumn().LogicalWriter<int>();
                valueWriter.WriteBatch(new[] { 1000 });
            }

            using var table = ParquetTable<EntityWithByte>.Factory.FromFileSystem(tempDir);
            
            Assert.Throws<OverflowException>(() => table.ToList());
        }
        finally
        {
            if (Directory.Exists(tempDir))
            {
                Directory.Delete(tempDir, true);
            }
        }
    }

    [Test]
    public void ByteProperty_WithNegativeValue_ThrowsOverflowException()
    {
        var tempDir = Path.Combine(Path.GetTempPath(), $"ParquetTypeMismatch_{Guid.NewGuid()}");
        Directory.CreateDirectory(tempDir);

        try
        {
            var filePath = Path.Combine(tempDir, "negative.parquet");
            
            using (var fileWriter = new ParquetFileWriter(filePath, [new Column<int>("value")]))
            {
                using var rowGroup = fileWriter.AppendRowGroup();
                using var valueWriter = rowGroup.NextColumn().LogicalWriter<int>();
                valueWriter.WriteBatch(new[] { -1 });
            }

            using var table = ParquetTable<EntityWithByte>.Factory.FromFileSystem(tempDir);
            
            Assert.Throws<OverflowException>(() => table.ToList());
        }
        finally
        {
            if (Directory.Exists(tempDir))
            {
                Directory.Delete(tempDir, true);
            }
        }
    }

    [Test]
    public void ShortProperty_WithValueOver32767_ThrowsOverflowException()
    {
        var tempDir = Path.Combine(Path.GetTempPath(), $"ParquetTypeMismatch_{Guid.NewGuid()}");
        Directory.CreateDirectory(tempDir);

        try
        {
            var filePath = Path.Combine(tempDir, "overflow_short.parquet");
            
            using (var fileWriter = new ParquetFileWriter(filePath, [new Column<int>("value")]))
            {
                using var rowGroup = fileWriter.AppendRowGroup();
                using var valueWriter = rowGroup.NextColumn().LogicalWriter<int>();
                valueWriter.WriteBatch(new[] { 40000 });
            }

            using var table = ParquetTable<EntityWithShort>.Factory.FromFileSystem(tempDir);
            
            Assert.Throws<OverflowException>(() => table.ToList());
        }
        finally
        {
            if (Directory.Exists(tempDir))
            {
                Directory.Delete(tempDir, true);
            }
        }
    }

    [Test]
    public void ByteProperty_WithValidValue_ConvertsSuccessfully()
    {
        var tempDir = Path.Combine(Path.GetTempPath(), $"ParquetTypeMismatch_{Guid.NewGuid()}");
        Directory.CreateDirectory(tempDir);

        try
        {
            var filePath = Path.Combine(tempDir, "valid.parquet");
            
            using (var fileWriter = new ParquetFileWriter(filePath, [new Column<int>("value")]))
            {
                using var rowGroup = fileWriter.AppendRowGroup();
                using var valueWriter = rowGroup.NextColumn().LogicalWriter<int>();
                valueWriter.WriteBatch(new[] { 200 });
            }

            using var table = ParquetTable<EntityWithByte>.Factory.FromFileSystem(tempDir);
            var results = table.ToList();
            
            Assert.That(results, Has.Count.EqualTo(1));
            Assert.That(results[0].Value, Is.EqualTo(200));
        }
        finally
        {
            if (Directory.Exists(tempDir))
            {
                Directory.Delete(tempDir, true);
            }
        }
    }

    [Test]
    public void StringProperty_WithNumericValue_ConvertsSuccessfully()
    {
        var tempDir = Path.Combine(Path.GetTempPath(), $"ParquetTypeMismatch_{Guid.NewGuid()}");
        Directory.CreateDirectory(tempDir);

        try
        {
            var filePath = Path.Combine(tempDir, "string_to_int.parquet");
            
            using (var fileWriter = new ParquetFileWriter(filePath, [new Column<string>("value")]))
            {
                using var rowGroup = fileWriter.AppendRowGroup();
                using var valueWriter = rowGroup.NextColumn().LogicalWriter<string>();
                valueWriter.WriteBatch(new[] { "123" });
            }

            using var table = ParquetTable<EntityWithInt>.Factory.FromFileSystem(tempDir);
            var results = table.ToList();
            
            Assert.That(results, Has.Count.EqualTo(1));
            Assert.That(results[0].Value, Is.EqualTo(123));
        }
        finally
        {
            if (Directory.Exists(tempDir))
            {
                Directory.Delete(tempDir, true);
            }
        }
    }

    [Test]
    public void IntProperty_WithNonNumericString_ThrowsFormatException()
    {
        var tempDir = Path.Combine(Path.GetTempPath(), $"ParquetTypeMismatch_{Guid.NewGuid()}");
        Directory.CreateDirectory(tempDir);

        try
        {
            var filePath = Path.Combine(tempDir, "invalid_string.parquet");
            
            using (var fileWriter = new ParquetFileWriter(filePath, [new Column<string>("value")]))
            {
                using var rowGroup = fileWriter.AppendRowGroup();
                using var valueWriter = rowGroup.NextColumn().LogicalWriter<string>();
                valueWriter.WriteBatch(new[] { "not a number" });
            }

            using var table = ParquetTable<EntityWithInt>.Factory.FromFileSystem(tempDir);
            
            Assert.Throws<FormatException>(() => table.ToList());
        }
        finally
        {
            if (Directory.Exists(tempDir))
            {
                Directory.Delete(tempDir, true);
            }
        }
    }

    [Test]
    public void IntProperty_WithValueOverInt32Max_ThrowsOverflowException()
    {
        var tempDir = Path.Combine(Path.GetTempPath(), $"ParquetTypeMismatch_{Guid.NewGuid()}");
        Directory.CreateDirectory(tempDir);

        try
        {
            var filePath = Path.Combine(tempDir, "overflow_int.parquet");
            
            using (var fileWriter = new ParquetFileWriter(filePath, [new Column<long>("value")]))
            {
                using var rowGroup = fileWriter.AppendRowGroup();
                using var valueWriter = rowGroup.NextColumn().LogicalWriter<long>();
                valueWriter.WriteBatch(new[] { 3000000000L });
            }

            using var table = ParquetTable<EntityWithInt>.Factory.FromFileSystem(tempDir);
            
            Assert.Throws<OverflowException>(() => table.ToList());
        }
        finally
        {
            if (Directory.Exists(tempDir))
            {
                Directory.Delete(tempDir, true);
            }
        }
    }

    [Test]
    public void IntProperty_WithNegativeValueBelowInt32Min_ThrowsOverflowException()
    {
        var tempDir = Path.Combine(Path.GetTempPath(), $"ParquetTypeMismatch_{Guid.NewGuid()}");
        Directory.CreateDirectory(tempDir);

        try
        {
            var filePath = Path.Combine(tempDir, "overflow_int_negative.parquet");
            
            using (var fileWriter = new ParquetFileWriter(filePath, [new Column<long>("value")]))
            {
                using var rowGroup = fileWriter.AppendRowGroup();
                using var valueWriter = rowGroup.NextColumn().LogicalWriter<long>();
                valueWriter.WriteBatch(new[] { -3000000000L });
            }

            using var table = ParquetTable<EntityWithInt>.Factory.FromFileSystem(tempDir);
            
            Assert.Throws<OverflowException>(() => table.ToList());
        }
        finally
        {
            if (Directory.Exists(tempDir))
            {
                Directory.Delete(tempDir, true);
            }
        }
    }

    [Test]
    public void IntProperty_WithValidLongValue_ConvertsSuccessfully()
    {
        var tempDir = Path.Combine(Path.GetTempPath(), $"ParquetTypeMismatch_{Guid.NewGuid()}");
        Directory.CreateDirectory(tempDir);

        try
        {
            var filePath = Path.Combine(tempDir, "valid_int.parquet");
            
            using (var fileWriter = new ParquetFileWriter(filePath, [new Column<long>("value")]))
            {
                using var rowGroup = fileWriter.AppendRowGroup();
                using var valueWriter = rowGroup.NextColumn().LogicalWriter<long>();
                valueWriter.WriteBatch(new[] { 1000000L });
            }

            using var table = ParquetTable<EntityWithInt>.Factory.FromFileSystem(tempDir);
            var results = table.ToList();
            
            Assert.That(results, Has.Count.EqualTo(1));
            Assert.That(results[0].Value, Is.EqualTo(1000000));
        }
        finally
        {
            if (Directory.Exists(tempDir))
            {
                Directory.Delete(tempDir, true);
            }
        }
    }

    [Test]
    public void DateProperty_WithDateLogicalType_ConvertsToDateTime()
    {
        var tempDir = Path.Combine(Path.GetTempPath(), $"ParquetDateConversion_{Guid.NewGuid()}");
        Directory.CreateDirectory(tempDir);

        try
        {
            var filePath = Path.Combine(tempDir, "dates.parquet");
            
            // Write a Date column using ParquetSharp's Date type
            using (var fileWriter = new ParquetFileWriter(filePath, [new Column<Date>("event_date")]))
            {
                using var rowGroup = fileWriter.AppendRowGroup();
                using var dateWriter = rowGroup.NextColumn().LogicalWriter<Date>();
                dateWriter.WriteBatch(new[] { new Date(2024, 12, 7) });
            }

            // Read it with an entity that has DateTime property
            using var table = ParquetTable<EntityWithDateTime>.Factory.FromFileSystem(tempDir);
            var results = table.ToList();
            
            Assert.That(results, Has.Count.EqualTo(1));
            Assert.That(results[0].EventDate, Is.EqualTo(new DateTime(2024, 12, 7)));
        }
        finally
        {
            if (Directory.Exists(tempDir))
            {
                Directory.Delete(tempDir, true);
            }
        }
    }

#if NET6_0_OR_GREATER
    [Test]
    public void DateProperty_WithDateLogicalType_ConvertsToDateOnly()
    {
        var tempDir = Path.Combine(Path.GetTempPath(), $"ParquetDateConversion_{Guid.NewGuid()}");
        Directory.CreateDirectory(tempDir);

        try
        {
            var filePath = Path.Combine(tempDir, "dates.parquet");
            
            // Write a Date column using ParquetSharp's Date type
            using (var fileWriter = new ParquetFileWriter(filePath, [new Column<Date>("event_date")]))
            {
                using var rowGroup = fileWriter.AppendRowGroup();
                using var dateWriter = rowGroup.NextColumn().LogicalWriter<Date>();
                dateWriter.WriteBatch(new[] { new Date(2024, 12, 7) });
            }

            // Read it with an entity that has DateOnly property
            using var table = ParquetTable<EntityWithDateOnly>.Factory.FromFileSystem(tempDir);
            var results = table.ToList();
            
            Assert.That(results, Has.Count.EqualTo(1));
            Assert.That(results[0].EventDate, Is.EqualTo(new DateOnly(2024, 12, 7)));
        }
        finally
        {
            if (Directory.Exists(tempDir))
            {
                Directory.Delete(tempDir, true);
            }
        }
    }

    [Test]
    public void NullableDateProperty_WithDateLogicalType_ConvertsToNullableDateOnly()
    {
        var tempDir = Path.Combine(Path.GetTempPath(), $"ParquetDateConversion_{Guid.NewGuid()}");
        Directory.CreateDirectory(tempDir);

        try
        {
            var filePath = Path.Combine(tempDir, "nullable_dates.parquet");
            
            // Write a nullable Date column
            using (var fileWriter = new ParquetFileWriter(filePath, [new Column<Date?>("event_date")]))
            {
                using var rowGroup = fileWriter.AppendRowGroup();
                using var dateWriter = rowGroup.NextColumn().LogicalWriter<Date?>();
                dateWriter.WriteBatch(new Date?[] { new Date(2024, 12, 7), null });
            }

            // Read it with an entity that has nullable DateOnly property
            using var table = ParquetTable<EntityWithNullableDateOnly>.Factory.FromFileSystem(tempDir);
            var results = table.ToList();
            
            Assert.That(results, Has.Count.EqualTo(2));
            Assert.That(results[0].EventDate, Is.EqualTo(new DateOnly(2024, 12, 7)));
            Assert.That(results[1].EventDate, Is.Null);
        }
        finally
        {
            if (Directory.Exists(tempDir))
            {
                Directory.Delete(tempDir, true);
            }
        }
    }
#endif
}

public class EntityWithByte
{
    [ParquetColumn("value")]
    public byte Value { get; set; }
}

public class EntityWithShort
{
    [ParquetColumn("value")]
    public short Value { get; set; }
}

public class EntityWithInt
{
    [ParquetColumn("value")]
    public int Value { get; set; }
}

public class EntityWithDateTime
{
    [ParquetColumn("event_date")]
    public DateTime EventDate { get; set; }
}

#if NET6_0_OR_GREATER
public class EntityWithDateOnly
{
    [ParquetColumn("event_date")]
    public DateOnly EventDate { get; set; }
}

public class EntityWithNullableDateOnly
{
    [ParquetColumn("event_date")]
    public DateOnly? EventDate { get; set; }
}
#endif

