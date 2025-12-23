using System.Collections.Concurrent;
using System.Linq.Expressions;
using System.Reflection;
using ParquetSharpLINQ.Common.Converter;

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
                return FilterValueNormalizer.NormalizePartitionValue;

            var methodInfo = typeof(ColumnValueConverter).GetMethod(
                nameof(ColumnValueConverter.ConvertFromString), 
                BindingFlags.Public | BindingFlags.Static);

            if (methodInfo == null)
                throw new InvalidOperationException("Could not find method ColumnValueConverter.ConvertFromString");

            var genericMethod = methodInfo.MakeGenericMethod(type);

            // Compile a strongly-typed delegate for ConvertFromString<TTarget>(string)
            var param = Expression.Parameter(typeof(string), "s");
            var call = Expression.Call(null, genericMethod, param);
            var convert = Expression.Convert(call, typeof(object));
            var lambda = Expression.Lambda<Func<string, object>>(convert, param);
            return lambda.Compile();
        }
    }
}
