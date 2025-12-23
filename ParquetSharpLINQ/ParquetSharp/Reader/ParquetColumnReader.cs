using System.Collections.Immutable;
using ParquetSharp;
using ParquetSharpLINQ.ParquetSharp.Buffers;

namespace ParquetSharpLINQ.ParquetSharp.Reader;

/// <summary>
/// Handles reading column data from Parquet files and converting to immutable arrays.
/// </summary>
internal static class ParquetColumnReader
{
    /// <summary>
    /// Reads a single column from a row group and returns it as a typed immutable array.
    /// </summary>
    public static ImmutableArray<T> ReadColumn<T>(
        RowGroupReader rowGroupReader,
        ParquetColumnResolver.ColumnReference reference,
        int numRows)
    {
        using var columnReader = rowGroupReader.Column(reference.Index);
        if (numRows == 0)
            return ImmutableArray<T>.Empty;

        var values = columnReader.LogicalReader<T>().ReadAll(numRows);
        return ImmutableArray.CreateRange(values);
    }

    // A small generic helper that builds the factory which reads and wraps the column values
    private static Func<RowGroupReader, ParquetColumnResolver.ColumnReference, int, IColumnBuffer> CreateFactory<T>()
        => (rowGroupReader, handle, numRows) => new ColumnBuffer<T>(ReadColumn<T>(rowGroupReader, handle, numRows));

    // Centralized mapping from CLR type to a factory that produces the appropriate IColumnBuffer
    private static readonly IReadOnlyDictionary<Type, Func<RowGroupReader, ParquetColumnResolver.ColumnReference, int, IColumnBuffer>> ColumnBufferFactories
        = new Dictionary<Type, Func<RowGroupReader, ParquetColumnResolver.ColumnReference, int, IColumnBuffer>>
    {
        { typeof(bool), CreateFactory<bool>() },
        { typeof(bool?), CreateFactory<bool?>() },
        { typeof(sbyte), CreateFactory<sbyte>() },
        { typeof(sbyte?), CreateFactory<sbyte?>() },
        { typeof(byte), CreateFactory<byte>() },
        { typeof(byte?), CreateFactory<byte?>() },
        { typeof(short), CreateFactory<short>() },
        { typeof(short?), CreateFactory<short?>() },
        { typeof(ushort), CreateFactory<ushort>() },
        { typeof(ushort?), CreateFactory<ushort?>() },
        { typeof(int), CreateFactory<int>() },
        { typeof(int?), CreateFactory<int?>() },
        { typeof(uint), CreateFactory<uint>() },
        { typeof(uint?), CreateFactory<uint?>() },
        { typeof(long), CreateFactory<long>() },
        { typeof(long?), CreateFactory<long?>() },
        { typeof(ulong), CreateFactory<ulong>() },
        { typeof(ulong?), CreateFactory<ulong?>() },
        { typeof(float), CreateFactory<float>() },
        { typeof(float?), CreateFactory<float?>() },
        { typeof(double), CreateFactory<double>() },
        { typeof(double?), CreateFactory<double?>() },
        { typeof(decimal), CreateFactory<decimal>() },
        { typeof(decimal?), CreateFactory<decimal?>() },
        { typeof(string), CreateFactory<string>() },
        { typeof(Date), CreateFactory<Date>() },
        { typeof(Date?), CreateFactory<Date?>() },
        { typeof(DateTime), CreateFactory<DateTime>() },
        { typeof(DateTime?), CreateFactory<DateTime?>() },
        { typeof(TimeSpan), CreateFactory<TimeSpan>() },
        { typeof(TimeSpan?), CreateFactory<TimeSpan?>() },
        { typeof(DateOnly), CreateFactory<DateOnly>() },
        { typeof(DateOnly?), CreateFactory<DateOnly?>() },
        { typeof(byte[]), CreateFactory<byte[]>() }
    };

    public static IColumnBuffer ReadColumnBuffer(
        RowGroupReader rowGroupReader,
        ParquetColumnResolver.ColumnReference reference,
        int numRows,
        Type targetType)
    {
        return ColumnBufferFactories.TryGetValue(targetType, out var factory) 
            ? factory(rowGroupReader, reference, numRows) 
            : throw new NotSupportedException($"Reading type {targetType.FullName} is not supported.");
    }
}
