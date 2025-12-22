using System.Globalization;
using System.Runtime.CompilerServices;

namespace ParquetSharpLINQ.ParquetSharp;

public static class ColumnValueConverter
{
    private static CultureInfo InvariantCulture => CultureInfo.InvariantCulture;
    private static DateTimeStyles DateTimeStyles => DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal;

    public static bool TryConvertFromInt64<TTarget>(long source, out TTarget? value)
    {
        var targetType = Nullable.GetUnderlyingType(typeof(TTarget)) ?? typeof(TTarget);

        if (targetType == typeof(long))
            return CastParsed(s => s, source, out value);
        if (targetType == typeof(float))
            return CastParsed(s => (float)s, source, out value);
        if (targetType == typeof(double))
            return CastParsed(s => (double)s, source, out value);
        if (targetType == typeof(decimal))
            return CastParsed(s => (decimal)s, source, out value);

        Unsafe.SkipInit(out value);
        return false;
    }
    
    public static bool TryConvertFromUInt64<TTarget>(ulong source, out TTarget? value)
    {
        var targetType = Nullable.GetUnderlyingType(typeof(TTarget)) ?? typeof(TTarget);

        if (targetType == typeof(ulong))
            return CastParsed(s => s, source, out value);
        if (targetType == typeof(float))
            return CastParsed(s => (float)s, source, out value);
        if (targetType == typeof(double))
            return CastParsed(s => (double)s, source, out value);
        if (targetType == typeof(decimal))
            return CastParsed(s => (decimal)s, source, out value);

        Unsafe.SkipInit(out value);
        return false;
    }

    public static bool TryConvertFromInt32<TTarget>(int source, out TTarget? value)
    {
        var targetType = Nullable.GetUnderlyingType(typeof(TTarget)) ?? typeof(TTarget);

        if (targetType == typeof(int))
            return CastParsed(s => s, source, out value);
        if (targetType == typeof(long))
            return CastParsed(s => (long)s, source, out value);
        if (targetType == typeof(float))
            return CastParsed(s => (float)s, source, out value);
        if (targetType == typeof(double))
            return CastParsed(s => (double)s, source, out value);
        if (targetType == typeof(decimal))
            return CastParsed(s => (decimal)s, source, out value);

        Unsafe.SkipInit(out value);
        return false;
    }
    
    public static bool TryConvertFromUInt32<TTarget>(uint source, out TTarget? value)
    {
        var targetType = Nullable.GetUnderlyingType(typeof(TTarget)) ?? typeof(TTarget);

        if (targetType == typeof(uint))
            return CastParsed(s => s, source, out value);
        if (targetType == typeof(ulong))
            return CastParsed(s => (ulong)s, source, out value);
        if (targetType == typeof(float))
            return CastParsed(s => (float)s, source, out value);
        if (targetType == typeof(double))
            return CastParsed(s => (double)s, source, out value);
        if (targetType == typeof(decimal))
            return CastParsed(s => (decimal)s, source, out value);

        Unsafe.SkipInit(out value);
        return false;
    }

    public static bool TryConvertFromInt16<TTarget>(short source, out TTarget? value)
    {
        var targetType = Nullable.GetUnderlyingType(typeof(TTarget)) ?? typeof(TTarget);

        if (targetType == typeof(short))
            return CastParsed(s => s, source, out value);
        if (targetType == typeof(int))
            return CastParsed(s => (int)s, source, out value);
        if (targetType == typeof(long))
            return CastParsed(s => (long)s, source, out value);
        if (targetType == typeof(float))
            return CastParsed(s => (float)s, source, out value);
        if (targetType == typeof(double))
            return CastParsed(s => (double)s, source, out value);
        if (targetType == typeof(decimal))
            return CastParsed(s => (decimal)s, source, out value);

        Unsafe.SkipInit(out value);
        return false;
    }
    
    public static bool TryConvertFromUInt16<TTarget>(ushort source, out TTarget? value)
    {
        var targetType = Nullable.GetUnderlyingType(typeof(TTarget)) ?? typeof(TTarget);

        if (targetType == typeof(ushort))
            return CastParsed(s => s, source, out value);
        if (targetType == typeof(uint))
            return CastParsed(s => (uint)s, source, out value);
        if (targetType == typeof(ulong))
            return CastParsed(s => (ulong)s, source, out value);
        if (targetType == typeof(float))
            return CastParsed(s => (float)s, source, out value);
        if (targetType == typeof(double))
            return CastParsed(s => (double)s, source, out value);
        if (targetType == typeof(decimal))
            return CastParsed(s => (decimal)s, source, out value);

        Unsafe.SkipInit(out value);
        return false;
    }

    public static bool TryConvertFromByte<TTarget>(byte source, out TTarget? value)
    {
        var targetType = Nullable.GetUnderlyingType(typeof(TTarget)) ?? typeof(TTarget);
        
        if (targetType == typeof(byte))
            return CastParsed(s => s, source, out value);
        if (targetType == typeof(short))
            return CastParsed(s => (short)s, source, out value);
        if (targetType == typeof(ushort))
            return CastParsed(s => (ushort)s, source, out value);
        if (targetType == typeof(int))
            return CastParsed(s => (int)s, source, out value);
        if (targetType == typeof(uint))
            return CastParsed(s => (uint)s, source, out value);
        if (targetType == typeof(long))
            return CastParsed(s => (long)s, source, out value);
        if (targetType == typeof(ulong))
            return CastParsed(s => (ulong)s, source, out value);
        if (targetType == typeof(float))
            return CastParsed(s => (float)s, source, out value);
        if (targetType == typeof(double))
            return CastParsed(s => (double)s, source, out value);
        if (targetType == typeof(decimal))
            return CastParsed(s => (decimal)s, source, out value);

        Unsafe.SkipInit(out value);
        return false;
    }
    
    public static bool TryConvertFromSByte<TTarget>(sbyte source, out TTarget? value)
    {
        var targetType = Nullable.GetUnderlyingType(typeof(TTarget)) ?? typeof(TTarget);

        if (targetType == typeof(sbyte))
            return CastParsed(s => s, source, out value);
        if (targetType == typeof(short))
            return CastParsed(s => (short)s, source, out value);
        if (targetType == typeof(int))
            return CastParsed(s => (int)s, source, out value);
        if (targetType == typeof(long))
            return CastParsed(s => (long)s, source, out value);
        if (targetType == typeof(float))
            return CastParsed(s => (float)s, source, out value);
        if (targetType == typeof(double))
            return CastParsed(s => (double)s, source, out value);
        if (targetType == typeof(decimal))
            return CastParsed(s => (decimal)s, source, out value);

        Unsafe.SkipInit(out value);
        return false;
    }
    
    public static bool TryConvertFromBoolean<TTarget>(bool source, out TTarget? value)
        {
            var targetType = Nullable.GetUnderlyingType(typeof(TTarget)) ?? typeof(TTarget);
    
            if (targetType == typeof(bool))
                return CastParsed(s => s, source, out value);
    
            Unsafe.SkipInit(out value);
            return false;
        }

    public static bool TryConvertFromDecimal<TTarget>(decimal source, out TTarget? value)
    {
        var targetType = Nullable.GetUnderlyingType(typeof(TTarget)) ?? typeof(TTarget);

        if (targetType == typeof(decimal))
            return CastParsed(s => s, source, out value);
        if (targetType == typeof(double))
            return CastParsed(s => (double)s, source, out value);

        Unsafe.SkipInit(out value);
        return false;
    }

    public static bool TryConvertFromDouble<TTarget>(double source, out TTarget? value)
    {
        var targetType = Nullable.GetUnderlyingType(typeof(TTarget)) ?? typeof(TTarget);

        if (targetType == typeof(double))
            return CastParsed(s => s, source, out value);
        if (targetType == typeof(decimal))
            return CastParsed(s => (decimal)s, source, out value);

        Unsafe.SkipInit(out value);
        return false;
    }

    public static bool TryConvertFromSingle<TTarget>(float source, out TTarget? value)
    {
        var targetType = Nullable.GetUnderlyingType(typeof(TTarget)) ?? typeof(TTarget);

        if (targetType == typeof(float))
            return CastParsed(s => s, source, out value);
        if (targetType == typeof(double))
            return CastParsed(s => (double)s, source, out value);
        if (targetType == typeof(decimal))
            return CastParsed(s => (decimal)s, source, out value);

        Unsafe.SkipInit(out value);
        return false;
    }
    
    public static bool TryConvertFromString<TTarget>(string? source, out TTarget? value)
    {
        if (source is null)
        {
            if (TargetAllowsNull<TTarget>())
            {
                value = default; // null
                return true;
            }

            Unsafe.SkipInit(out value);
            return false;
        }

        var targetType = Nullable.GetUnderlyingType(typeof(TTarget)) ?? typeof(TTarget);

        if (targetType == typeof(byte))
            return CastParsed(s => byte.Parse(s, InvariantCulture), source, out value);
        if (targetType == typeof(short))
            return CastParsed(s => short.Parse(s, InvariantCulture), source, out value);
        if (targetType == typeof(int))
            return CastParsed(s => int.Parse(s, InvariantCulture), source, out value);
        if (targetType == typeof(long))
            return CastParsed(s => long.Parse(s, InvariantCulture), source, out value);
        if (targetType == typeof(float))
            return CastParsed(s => float.Parse(s, InvariantCulture), source, out value);
        if (targetType == typeof(double))
            return CastParsed(s => double.Parse(s, InvariantCulture), source, out value);
        if (targetType == typeof(decimal))
            return CastParsed(s => decimal.Parse(s, InvariantCulture), source, out value);
        if (targetType == typeof(bool))
            return CastParsed(bool.Parse, source, out value);
        if (targetType == typeof(DateOnly))
            return CastParsed(s => DateOnly.Parse(s, InvariantCulture), source, out value);
        if (targetType == typeof(TimeOnly))
            return CastParsed(s => TimeOnly.Parse(s, InvariantCulture), source, out value);
        if (targetType == typeof(DateTime))
            return CastParsed(s => DateTime.Parse(s, InvariantCulture, DateTimeStyles), source, out value);
        if (targetType == typeof(TimeSpan))
            return CastParsed(s => TimeSpan.Parse(s, InvariantCulture), source, out value);
        if (targetType == typeof(Guid))
            return CastParsed(Guid.Parse, source, out value);
        if (targetType == typeof(string))
        {
            value = Unsafe.As<string, TTarget>(ref source);
            return true;
        }

        Unsafe.SkipInit(out value);
        return false;
    }

    public static bool TryConvertFromParquetDate<TTarget>(global::ParquetSharp.Date date, out TTarget? value)
    {
        var targetType = Nullable.GetUnderlyingType(typeof(TTarget)) ?? typeof(TTarget);

        if (targetType == typeof(global::ParquetSharp.Date))
            return CastParsed(s => s, date, out value);
        if (targetType == typeof(DateTime))
            return CastParsed(s => s.DateTime, date, out value);
        if (targetType == typeof(DateOnly))
            return CastParsed(s => DateOnly.FromDateTime(s.DateTime), date, out value);

        Unsafe.SkipInit(out value);
        return false;
    }
    
    public static bool TryConvertFromDateTime<TTarget>(DateTime source, out TTarget? value)
    {
        var targetType = Nullable.GetUnderlyingType(typeof(TTarget)) ?? typeof(TTarget);

        if (targetType == typeof(DateTime))
            return CastParsed(s => s, source, out value);
        if (targetType == typeof(DateOnly))
            return CastParsed(DateOnly.FromDateTime, source, out value);
        if (targetType == typeof(global::ParquetSharp.Date))
            return CastParsed(s => new global::ParquetSharp.Date(s), source, out value);

        Unsafe.SkipInit(out value);
        return false;
    }
    
    public static bool TryConvertFromTimeSpan<TTarget>(TimeSpan source, out TTarget? value)
    {
        var targetType = Nullable.GetUnderlyingType(typeof(TTarget)) ?? typeof(TTarget);

        if (targetType == typeof(TimeSpan))
            return CastParsed(s => s, source, out value);

        Unsafe.SkipInit(out value);
        return false;
    }
    
    public static bool TryConvertFromDateOnly<TTarget>(DateOnly source, out TTarget? value)
    {
        var targetType = Nullable.GetUnderlyingType(typeof(TTarget)) ?? typeof(TTarget);

        if (targetType == typeof(DateOnly))
            return CastParsed(s => s, source, out value);
        if (targetType == typeof(DateTime))
            return CastParsed(s => s.ToDateTime(TimeOnly.MinValue), source, out value);
        if (targetType == typeof(global::ParquetSharp.Date))
            return CastParsed(s => new global::ParquetSharp.Date(s.ToDateTime(TimeOnly.MinValue)), source, out value);

        Unsafe.SkipInit(out value);
        return false;
    }
    private static bool TargetAllowsNull<TTarget>()
    {
        var targetType = typeof(TTarget);
        var isNullable = Nullable.GetUnderlyingType(targetType) != null;
        return isNullable;
    }

    private static bool CastParsed<TSource, TParsed, TTarget>(
        Func<TSource, TParsed> parser,
        TSource source,
        out TTarget? value)
    {
        var parsed = parser(source);
        value = (TTarget?)(object?)parsed;
        return true;
    }
}