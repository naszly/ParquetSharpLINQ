using ParquetSharp;

namespace ParquetSharpLINQ.ParquetSharp;

/// <summary>
/// Handles reading column data from Parquet files and converting to object arrays.
/// </summary>
internal static class ParquetColumnReader
{
    private const string SupportedTypesMessage =
        "Supported types: bool, sbyte, byte, short, ushort, int, uint, long, ulong, " +
        "float, double, decimal, string, DateTime, DateOnly, TimeSpan, byte[]";

    /// <summary>
    /// Reads a single column from a row group.
    /// </summary>
    public static object?[] ReadColumn(
        RowGroupReader rowGroupReader,
        ParquetColumnMapper.ColumnHandle handle,
        int numRows)
    {
        using var columnReader = rowGroupReader.Column(handle.Index);
        if (numRows == 0) return [];

        var targetType = ParquetTypeResolver.ResolveClrType(handle.Descriptor);
        if (ParquetTypeResolver.ShouldUseNullable(handle.Descriptor) && targetType.IsValueType &&
            Nullable.GetUnderlyingType(targetType) == null)
        {
            targetType = typeof(Nullable<>).MakeGenericType(targetType);
        }

        var typedValues = ReadColumnData(columnReader, targetType, numRows);
        return ConvertToObjectArray(typedValues);
    }

    /// <summary>
    /// Reads typed column data from a column reader.
    /// Uses reflection to call the generic LogicalReader method with the correct type.
    /// </summary>
    private static Array ReadColumnData(ColumnReader columnReader, Type targetType, int numRows)
    {
        var logicalReaderMethod = typeof(ColumnReader)
            .GetMethods()
            .Single(m => m.Name == nameof(ColumnReader.LogicalReader) 
                         && m.IsGenericMethodDefinition 
                         && m.GetParameters().Length == 1
                         && m.GetParameters()[0].ParameterType == typeof(int))
            .MakeGenericMethod(targetType);
        const int bufferLength = 4096;
        var logicalReader = logicalReaderMethod.Invoke(columnReader, [bufferLength]);
        
        var readAllMethod = logicalReader!.GetType().GetMethod(nameof(LogicalColumnReader<>.ReadAll))!;
        var result = readAllMethod.Invoke(logicalReader, [numRows]);

        return (Array)result!;
    }

    /// <summary>
    /// Converts a typed array to an object array.
    /// </summary>
    private static object?[] ConvertToObjectArray(Array typedValues)
    {
        var result = new object?[typedValues.Length];
        for (var i = 0; i < typedValues.Length; ++i)
        {
            result[i] = typedValues.GetValue(i);
        }
        return result;
    }

    /// <summary>
    /// Ensures the values array has the expected row count, padding with nulls if necessary.
    /// </summary>
    public static object?[] EnsureRowCount(object?[] values, int expectedCount)
    {
        if (values.Length == expectedCount) return values;
        
        var result = new object?[expectedCount];
        Array.Copy(values, result, Math.Min(values.Length, expectedCount));
        return result;
    }
}
