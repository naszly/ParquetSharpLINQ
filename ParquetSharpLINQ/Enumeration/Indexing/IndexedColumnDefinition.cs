using System.Collections;
using System.Linq.Expressions;
using System.Reflection;
using ParquetSharpLINQ.Attributes;
using ParquetSharpLINQ.Mappers;

namespace ParquetSharpLINQ.Enumeration.Indexing;

internal sealed class IndexedColumnDefinition
{
    private IndexedColumnDefinition(
        string columnName,
        PropertyInfo property,
        IComparer<object?> comparer,
        Func<object?, object?> converter,
        bool isNullable)
    {
        ColumnName = columnName;
        Property = property;
        Comparer = comparer;
        Converter = converter;
        IsNullable = isNullable;
    }

    public string ColumnName { get; }
    public PropertyInfo Property { get; }
    public IComparer<object?> Comparer { get; }
    public Func<object?, object?> Converter { get; }
    public bool IsNullable { get; }

    public static IndexedColumnDefinition Create(PropertyInfo property)
    {
        var attr = property.GetCustomAttribute<ParquetColumnAttribute>();
        if (attr is not { Indexed: true })
            throw new InvalidOperationException($"Property '{property.Name}' is not indexed.");

        var columnName = string.IsNullOrWhiteSpace(attr.Name) ? property.Name : attr.Name;
        var comparer = CreateComparer(property.PropertyType, attr.ComparerType);
        var converter = CreateConverter(property.PropertyType);
        var isNullable = IsNullableProperty(property);

        return new IndexedColumnDefinition(columnName, property, comparer, converter, isNullable);
    }

    private static Func<object?, object?> CreateConverter(Type propertyType)
    {
        var valueParam = Expression.Parameter(typeof(object), "value");
        var convertMethod = typeof(ParquetMapperHelpers)
            .GetMethod(nameof(ParquetMapperHelpers.ConvertValue))!
            .MakeGenericMethod(propertyType);
        var call = Expression.Call(convertMethod, valueParam);
        var body = Expression.Convert(call, typeof(object));
        return Expression.Lambda<Func<object?, object?>>(body, valueParam).Compile();
    }

    private static IComparer<object?> CreateComparer(Type propertyType, Type? comparerType)
    {
        if (comparerType == null)
        {
            var comparable = typeof(IComparable).IsAssignableFrom(propertyType);
            var genericComparable = propertyType
                .GetInterfaces()
                .Any(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IComparable<>));

            if (!comparable && !genericComparable)
            {
                throw new InvalidOperationException(
                    $"Indexed property type '{propertyType.FullName}' must implement IComparable or specify ComparerType.");
            }

            var defaultComparerType = typeof(Comparer<>).MakeGenericType(propertyType);
            var defaultComparer = (IComparer)defaultComparerType
                .GetProperty(nameof(Comparer<int>.Default))!
                .GetValue(null)!;
            return new ObjectComparerAdapter(defaultComparer);
        }

        if (!typeof(IComparer).IsAssignableFrom(comparerType))
        {
            var genericComparerInterface = comparerType
                .GetInterfaces()
                .FirstOrDefault(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IComparer<>));

            if (genericComparerInterface == null)
            {
                throw new InvalidOperationException(
                    $"Comparer type '{comparerType.FullName}' must implement IComparer or IComparer<T>.");
            }
        }

        var comparerInstance = Activator.CreateInstance(comparerType)
            ?? throw new InvalidOperationException($"Could not create comparer '{comparerType.FullName}'.");

        if (comparerInstance is IComparer nonGenericComparer)
            return new ObjectComparerAdapter(nonGenericComparer);

        var comparerAdapterType = typeof(GenericComparerAdapter<>)
            .MakeGenericType(propertyType);
        return (IComparer<object?>)Activator.CreateInstance(comparerAdapterType, comparerInstance)!;
    }

    private static bool IsNullableProperty(PropertyInfo property)
    {
        var type = property.PropertyType;
        if (type.IsValueType)
            return Nullable.GetUnderlyingType(type) != null;

        var nullability = new NullabilityInfoContext().Create(property);
        return nullability.ReadState == NullabilityState.Nullable;
    }

    private sealed class ObjectComparerAdapter : IComparer<object?>
    {
        private readonly IComparer _comparer;

        public ObjectComparerAdapter(IComparer comparer)
        {
            _comparer = comparer;
        }

        public int Compare(object? x, object? y)
        {
            return _comparer.Compare(x, y);
        }
    }

    private sealed class GenericComparerAdapter<T> : IComparer<object?>
    {
        private readonly IComparer<T> _comparer;

        public GenericComparerAdapter(IComparer<T> comparer)
        {
            _comparer = comparer;
        }

        public int Compare(object? x, object? y)
        {
            if (x is null)
            {
                return y is null ? 0 : -1;
            }

            if (y is null)
            {
                return 1;
            }

            return _comparer.Compare((T)x, (T)y);
        }
    }
}
