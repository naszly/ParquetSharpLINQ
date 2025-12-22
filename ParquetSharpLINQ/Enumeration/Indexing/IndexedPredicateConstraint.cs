namespace ParquetSharpLINQ.Enumeration.Indexing;

public interface IIndexedPredicateConstraint
{
    IIndexedColumnDefinition Definition { get; }

    bool RowGroupMayMatch(RowGroupValues values);

    bool TryCountMatches(RowGroupValues values, out int count);
}

public abstract class IndexedPredicateConstraint<T> : IIndexedPredicateConstraint
{
    protected IndexedPredicateConstraint(IIndexedColumnDefinition definition)
    {
        Definition = definition;
    }

    public IIndexedColumnDefinition Definition { get; }

    public bool RowGroupMayMatch(RowGroupValues values)
    {
        return RowGroupMayMatch(GetTypedValues(values));
    }

    public bool TryCountMatches(RowGroupValues values, out int count)
    {
        return TryCountMatches(GetTypedValues(values), out count);
    }

    protected abstract bool RowGroupMayMatch(SortedValueArray<T> values);

    protected abstract bool TryCountMatches(SortedValueArray<T> values, out int count);

    private static SortedValueArray<T> GetTypedValues(RowGroupValues values)
    {
        if (values is RowGroupValues<T> typed)
            return typed.Values;

        throw new InvalidOperationException(
            $"Indexed predicate expects values of type '{typeof(T).FullName}', but got '{values.ValueType.FullName}'.");
    }
}

public sealed class AlwaysMatchConstraint<T> : IndexedPredicateConstraint<T>
{
    public AlwaysMatchConstraint(IIndexedColumnDefinition definition) : base(definition)
    {
    }

    protected override bool RowGroupMayMatch(SortedValueArray<T> values)
    {
        return true;
    }

    protected override bool TryCountMatches(SortedValueArray<T> values, out int count)
    {
        count = values.Count;
        return true;
    }
}

public sealed class EqualsConstraint<T> : IndexedPredicateConstraint<T>
{
    private readonly T? _value;

    public EqualsConstraint(IIndexedColumnDefinition definition, T? value) : base(definition)
    {
        _value = value;
    }

    protected override bool RowGroupMayMatch(SortedValueArray<T> values)
    {
        return values.Contains(_value);
    }

    protected override bool TryCountMatches(SortedValueArray<T> values, out int count)
    {
        count = values.CountEquals(_value);
        return true;
    }
}

public sealed class NotEqualsConstraint<T> : IndexedPredicateConstraint<T>
{
    private readonly T? _value;

    public NotEqualsConstraint(IIndexedColumnDefinition definition, T? value) : base(definition)
    {
        _value = value;
    }

    protected override bool RowGroupMayMatch(SortedValueArray<T> values)
    {
        return values.HasAnyValueNotEqual(_value);
    }

    protected override bool TryCountMatches(SortedValueArray<T> values, out int count)
    {
        count = values.Count - values.CountEquals(_value);
        return true;
    }
}

public enum ComparisonKind
{
    GreaterThan,
    GreaterThanOrEqual,
    LessThan,
    LessThanOrEqual
}

public sealed class ComparisonConstraint<T> : IndexedPredicateConstraint<T>
{
    private readonly T? _value;
    private readonly ComparisonKind _kind;

    public ComparisonConstraint(IIndexedColumnDefinition definition, T? value, ComparisonKind kind) : base(definition)
    {
        _value = value;
        _kind = kind;
    }

    protected override bool RowGroupMayMatch(SortedValueArray<T> values)
    {
        return _kind switch
        {
            ComparisonKind.GreaterThan => values.IntersectsRange(_value, true, false, default, false, true),
            ComparisonKind.GreaterThanOrEqual => values.IntersectsRange(_value, true, true, default, false, true),
            ComparisonKind.LessThan => values.IntersectsRange(default, false, true, _value, true, false),
            ComparisonKind.LessThanOrEqual => values.IntersectsRange(default, false, true, _value, true, true),
            _ => true
        };
    }

    protected override bool TryCountMatches(SortedValueArray<T> values, out int count)
    {
        count = _kind switch
        {
            ComparisonKind.GreaterThan => values.CountInRange(_value, true, false, default, false, true),
            ComparisonKind.GreaterThanOrEqual => values.CountInRange(_value, true, true, default, false, true),
            ComparisonKind.LessThan => values.CountInRange(default, false, true, _value, true, false),
            ComparisonKind.LessThanOrEqual => values.CountInRange(default, false, true, _value, true, true),
            _ => 0
        };
        return true;
    }
}

public sealed class StartsWithConstraint : IndexedPredicateConstraint<string>
{
    private readonly string _min;
    private readonly string? _maxExclusive;

    private StartsWithConstraint(IIndexedColumnDefinition definition, string min, string? maxExclusive)
        : base(definition)
    {
        _min = min;
        _maxExclusive = maxExclusive;
    }

    public static StartsWithConstraint? TryCreate(IIndexedColumnDefinition definition, string prefix)
    {
        if (definition.PropertyType != typeof(string))
            return null;

        var max = GetNextPrefix(prefix);
        return new StartsWithConstraint(definition, prefix, max);
    }

    protected override bool RowGroupMayMatch(SortedValueArray<string> values)
    {
        var maxExclusive = _maxExclusive ?? string.Empty;
        return values.IntersectsRange(_min, true, true, maxExclusive, _maxExclusive != null, false);
    }

    protected override bool TryCountMatches(SortedValueArray<string> values, out int count)
    {
        var maxExclusive = _maxExclusive ?? string.Empty;
        count = values.CountInRange(_min, true, true, maxExclusive, _maxExclusive != null, false);
        return true;
    }

    private static string? GetNextPrefix(string prefix)
    {
        if (prefix.Length == 0)
            return null;

        var chars = prefix.ToCharArray();
        for (var i = chars.Length - 1; i >= 0; i--)
        {
            if (chars[i] == char.MaxValue)
                continue;

            chars[i]++;
            for (var j = i + 1; j < chars.Length; j++)
                chars[j] = char.MinValue;

            return new string(chars);
        }

        return null;
    }
}
