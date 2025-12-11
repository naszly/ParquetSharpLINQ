using System.Collections.Immutable;
using ParquetSharp;

namespace ParquetSharpLINQ.ParquetSharp;

/// <summary>
/// Handles reading column data from Parquet files and converting to immutable arrays.
/// </summary>
internal static class ParquetColumnReader
{
    private const string SupportedTypesMessage = 
        "Supported types: bool, sbyte, byte, short, ushort, int, uint, long, ulong, float, double, decimal, string, DateTime, Date, DateOnly, TimeSpan, byte[]";

    /// <summary>
    /// Reads a single column from a row group and returns it as an immutable array of objects.
    /// </summary>
    public static ImmutableArray<object?> ReadColumn(
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

        return ReadColumnData(columnReader, targetType, numRows);
    }

    private static ImmutableArray<object?> ReadColumnData(ColumnReader columnReader, Type targetType, int numRows)
    {
        var underlyingType = Nullable.GetUnderlyingType(targetType);
        
        if (underlyingType != null)
        {
            return underlyingType switch
            {
                _ when underlyingType == typeof(bool) =>
                    ConvertToObjectArray(columnReader.LogicalReader<bool?>().ReadAll(numRows)),
                _ when underlyingType == typeof(sbyte) =>
                    ConvertToObjectArray(columnReader.LogicalReader<sbyte?>().ReadAll(numRows)),
                _ when underlyingType == typeof(byte) =>
                    ConvertToObjectArray(columnReader.LogicalReader<byte?>().ReadAll(numRows)),
                _ when underlyingType == typeof(short) =>
                    ConvertToObjectArray(columnReader.LogicalReader<short?>().ReadAll(numRows)),
                _ when underlyingType == typeof(ushort) =>
                    ConvertToObjectArray(columnReader.LogicalReader<ushort?>().ReadAll(numRows)),
                _ when underlyingType == typeof(int) =>
                    ConvertToObjectArray(columnReader.LogicalReader<int?>().ReadAll(numRows)),
                _ when underlyingType == typeof(uint) =>
                    ConvertToObjectArray(columnReader.LogicalReader<uint?>().ReadAll(numRows)),
                _ when underlyingType == typeof(long) =>
                    ConvertToObjectArray(columnReader.LogicalReader<long?>().ReadAll(numRows)),
                _ when underlyingType == typeof(ulong) =>
                    ConvertToObjectArray(columnReader.LogicalReader<ulong?>().ReadAll(numRows)),
                _ when underlyingType == typeof(float) =>
                    ConvertToObjectArray(columnReader.LogicalReader<float?>().ReadAll(numRows)),
                _ when underlyingType == typeof(double) =>
                    ConvertToObjectArray(columnReader.LogicalReader<double?>().ReadAll(numRows)),
                _ when underlyingType == typeof(decimal) =>
                    ConvertToObjectArray(columnReader.LogicalReader<decimal?>().ReadAll(numRows)),
                _ when underlyingType == typeof(Date) =>
                    ConvertToObjectArray(columnReader.LogicalReader<Date?>().ReadAll(numRows)),
                _ when underlyingType == typeof(DateTime) =>
                    ConvertToObjectArray(columnReader.LogicalReader<DateTime?>().ReadAll(numRows)),
                _ when underlyingType == typeof(DateOnly) =>
                    ConvertToObjectArray(columnReader.LogicalReader<DateOnly?>().ReadAll(numRows)),
                _ when underlyingType == typeof(TimeSpan) =>
                    ConvertToObjectArray(columnReader.LogicalReader<TimeSpan?>().ReadAll(numRows)),
                _ => throw new NotSupportedException($"Reading nullable type {targetType} is not supported. {SupportedTypesMessage}")
            };
        }

        return targetType switch
        {
            _ when targetType == typeof(bool) =>
                ConvertToObjectArray(columnReader.LogicalReader<bool>().ReadAll(numRows)),
            _ when targetType == typeof(sbyte) =>
                ConvertToObjectArray(columnReader.LogicalReader<sbyte>().ReadAll(numRows)),
            _ when targetType == typeof(byte) =>
                ConvertToObjectArray(columnReader.LogicalReader<byte>().ReadAll(numRows)),
            _ when targetType == typeof(short) =>
                ConvertToObjectArray(columnReader.LogicalReader<short>().ReadAll(numRows)),
            _ when targetType == typeof(ushort) =>
                ConvertToObjectArray(columnReader.LogicalReader<ushort>().ReadAll(numRows)),
            _ when targetType == typeof(int) =>
                ConvertToObjectArray(columnReader.LogicalReader<int>().ReadAll(numRows)),
            _ when targetType == typeof(uint) =>
                ConvertToObjectArray(columnReader.LogicalReader<uint>().ReadAll(numRows)),
            _ when targetType == typeof(long) =>
                ConvertToObjectArray(columnReader.LogicalReader<long>().ReadAll(numRows)),
            _ when targetType == typeof(ulong) =>
                ConvertToObjectArray(columnReader.LogicalReader<ulong>().ReadAll(numRows)),
            _ when targetType == typeof(float) =>
                ConvertToObjectArray(columnReader.LogicalReader<float>().ReadAll(numRows)),
            _ when targetType == typeof(double) =>
                ConvertToObjectArray(columnReader.LogicalReader<double>().ReadAll(numRows)),
            _ when targetType == typeof(decimal) =>
                ConvertToObjectArray(columnReader.LogicalReader<decimal>().ReadAll(numRows)),
            _ when targetType == typeof(string) =>
                ConvertToObjectArray(columnReader.LogicalReader<string>().ReadAll(numRows)),
            _ when targetType == typeof(Date) =>
                ConvertToObjectArray(columnReader.LogicalReader<Date>().ReadAll(numRows)),
            _ when targetType == typeof(DateTime) =>
                ConvertToObjectArray(columnReader.LogicalReader<DateTime>().ReadAll(numRows)),
            _ when targetType == typeof(DateOnly) =>
                ConvertToObjectArray(columnReader.LogicalReader<DateOnly>().ReadAll(numRows)),
            _ when targetType == typeof(TimeSpan) =>
                ConvertToObjectArray(columnReader.LogicalReader<TimeSpan>().ReadAll(numRows)),
            _ when targetType == typeof(byte[]) =>
                ConvertToObjectArray(columnReader.LogicalReader<byte[]>().ReadAll(numRows)),
            _ => throw new NotSupportedException($"Reading type {targetType} is not supported. {SupportedTypesMessage}")
        };
    }

    private static ImmutableArray<object?> ConvertToObjectArray<T>(T[] typedArray)
    {
        var builder = ImmutableArray.CreateBuilder<object?>(typedArray.Length);
        builder.Count = typedArray.Length;
        
        for (var i = 0; i < typedArray.Length; i++)
        {
            builder[i] = typedArray[i];
        }
        
        return builder.MoveToImmutable();
    }
}
