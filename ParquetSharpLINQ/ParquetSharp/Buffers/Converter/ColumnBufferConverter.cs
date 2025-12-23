using System.Runtime.CompilerServices;
using ParquetSharp;

namespace ParquetSharpLINQ.ParquetSharp.Buffers.Converter;

internal static class ColumnBufferConverter
{
    public static TTarget Convert<TSource, TTarget>(TSource source)
    {
        if (source is null && Nullable.GetUnderlyingType(typeof(TTarget)) != null)
        {
            return default!; // null is valid for TTarget
        }

        if (TryConvertValue(source, out TTarget? value) && value is not null)
        {
            return value;
        }

        throw new InvalidOperationException($"Cannot convert value {source} from {typeof(TSource)} to {typeof(TTarget)}.");
    }

    private static bool TryConvertValue<TSource, TTarget>(TSource source, out TTarget? value)
    {
        switch (source)
        {
            case bool b: return ColumnValueConverter.TryConvertFromBoolean(b, out value);
            case byte by: return ColumnValueConverter.TryConvertFromByte(by, out value);
            case sbyte sb: return ColumnValueConverter.TryConvertFromSByte(sb, out value);
            case short s: return ColumnValueConverter.TryConvertFromInt16(s, out value);
            case ushort us: return ColumnValueConverter.TryConvertFromUInt16(us, out value);
            case int i: return ColumnValueConverter.TryConvertFromInt32(i, out value);
            case uint ui: return ColumnValueConverter.TryConvertFromUInt32(ui, out value);
            case long l: return ColumnValueConverter.TryConvertFromInt64(l, out value);
            case ulong ul: return ColumnValueConverter.TryConvertFromUInt64(ul, out value);
            case decimal dec: return ColumnValueConverter.TryConvertFromDecimal(dec, out value);
            case double dbl: return ColumnValueConverter.TryConvertFromDouble(dbl, out value);
            case float f: return ColumnValueConverter.TryConvertFromSingle(f, out value);
            case string str: return ColumnValueConverter.TryConvertFromString(str, out value);
            case Date dtParquet: return ColumnValueConverter.TryConvertFromParquetDate(dtParquet, out value);
            case DateTime dt: return ColumnValueConverter.TryConvertFromDateTime(dt, out value);
            case DateOnly dateOnly: return ColumnValueConverter.TryConvertFromDateOnly(dateOnly, out value);
            case TimeSpan timeSpan: return ColumnValueConverter.TryConvertFromTimeSpan(timeSpan, out value);
            default:
                Unsafe.SkipInit(out value);
                return false;
        }
    }
}
