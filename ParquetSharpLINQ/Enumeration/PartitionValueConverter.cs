using System.Collections.Concurrent;
using System.Globalization;

namespace ParquetSharpLINQ.Enumeration;

internal static partial class PartitionFilter
{
    private static class PartitionValueConverter
    {
        private static readonly ConcurrentDictionary<Type, Func<string, object>> Converters = new();

        public static object Convert(string value, Type targetType)
        {
            var underlyingType = Nullable.GetUnderlyingType(targetType) ?? targetType;
            var converter = Converters.GetOrAdd(underlyingType, CreateConverter);
            return converter(value);
        }

        private static Func<string, object> CreateConverter(Type type)
        {
            if (type == typeof(string))
                return value => FilterValueNormalizer.NormalizePartitionValue(value);

            if (type == typeof(DateTime))
                return value => DateTime.Parse(value);

            if (type == typeof(DateTimeOffset))
                return value => DateTimeOffset.Parse(value);

            if (type == typeof(DateOnly))
                return value => DateOnly.Parse(value);

            if (type == typeof(TimeOnly))
                return value => TimeOnly.Parse(value);

            if (type == typeof(Guid))
                return value => Guid.Parse(value);

            if (type == typeof(bool))
                return value => bool.Parse(value);

            if (type == typeof(byte))
                return value => byte.Parse(value, NumberStyles.Integer, CultureInfo.InvariantCulture);
            if (type == typeof(sbyte))
                return value => sbyte.Parse(value, NumberStyles.Integer, CultureInfo.InvariantCulture);
            if (type == typeof(short))
                return value => short.Parse(value, NumberStyles.Integer, CultureInfo.InvariantCulture);
            if (type == typeof(ushort))
                return value => ushort.Parse(value, NumberStyles.Integer, CultureInfo.InvariantCulture);
            if (type == typeof(int))
                return value => int.Parse(value, NumberStyles.Integer, CultureInfo.InvariantCulture);
            if (type == typeof(uint))
                return value => uint.Parse(value, NumberStyles.Integer, CultureInfo.InvariantCulture);
            if (type == typeof(long))
                return value => long.Parse(value, NumberStyles.Integer, CultureInfo.InvariantCulture);
            if (type == typeof(ulong))
                return value => ulong.Parse(value, NumberStyles.Integer, CultureInfo.InvariantCulture);
            if (type == typeof(float))
                return value => float.Parse(value, NumberStyles.Float | NumberStyles.AllowThousands, CultureInfo.InvariantCulture);
            if (type == typeof(double))
                return value => double.Parse(value, NumberStyles.Float | NumberStyles.AllowThousands, CultureInfo.InvariantCulture);
            if (type == typeof(decimal))
                return value => decimal.Parse(value, NumberStyles.Number, CultureInfo.InvariantCulture);

            if (type.IsEnum)
                return value => Enum.Parse(type, value, ignoreCase: true);

            return _ => throw new NotSupportedException($"Unsupported type {type.FullName}");
        }
    }
}
