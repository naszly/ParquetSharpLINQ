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
    /// </summary>
    private static Array ReadColumnData(ColumnReader columnReader, Type targetType, int numRows)
    {
        var underlyingType = Nullable.GetUnderlyingType(targetType) ?? targetType;

        return underlyingType switch
        {
            _ when underlyingType == typeof(bool) => columnReader.LogicalReader<bool>().ReadAll(numRows),
            _ when underlyingType == typeof(sbyte) => columnReader.LogicalReader<sbyte>().ReadAll(numRows),
            _ when underlyingType == typeof(byte) => columnReader.LogicalReader<byte>().ReadAll(numRows),
            _ when underlyingType == typeof(short) => columnReader.LogicalReader<short>().ReadAll(numRows),
            _ when underlyingType == typeof(ushort) => columnReader.LogicalReader<ushort>().ReadAll(numRows),
            _ when underlyingType == typeof(int) => columnReader.LogicalReader<int>().ReadAll(numRows),
            _ when underlyingType == typeof(uint) => columnReader.LogicalReader<uint>().ReadAll(numRows),
            _ when underlyingType == typeof(long) => columnReader.LogicalReader<long>().ReadAll(numRows),
            _ when underlyingType == typeof(ulong) => columnReader.LogicalReader<ulong>().ReadAll(numRows),
            _ when underlyingType == typeof(float) => columnReader.LogicalReader<float>().ReadAll(numRows),
            _ when underlyingType == typeof(double) => columnReader.LogicalReader<double>().ReadAll(numRows),
            _ when underlyingType == typeof(decimal) => columnReader.LogicalReader<decimal>().ReadAll(numRows),
            _ when underlyingType == typeof(string) => columnReader.LogicalReader<string>().ReadAll(numRows),
            _ when underlyingType == typeof(DateTime) => columnReader.LogicalReader<DateTime>().ReadAll(numRows),
            _ when underlyingType == typeof(DateOnly) => columnReader.LogicalReader<DateOnly>().ReadAll(numRows),
            _ when underlyingType == typeof(TimeSpan) => columnReader.LogicalReader<TimeSpan>().ReadAll(numRows),
            _ when underlyingType == typeof(byte[]) => columnReader.LogicalReader<byte[]>().ReadAll(numRows),
            _ => throw new NotSupportedException($"Reading type {targetType} is not supported. {SupportedTypesMessage}")
        };
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

