namespace ParquetSharpLINQ.Enumeration.Indexing;

internal abstract class IndexedPredicateConstraint
{
    protected IndexedPredicateConstraint(IndexedColumnDefinition definition)
    {
        Definition = definition;
    }

    public IndexedColumnDefinition Definition { get; }

    public abstract bool RowGroupMayMatch(SortedValueArray values);

    public abstract bool TryCountMatches(SortedValueArray values, out int count);
}

internal sealed class AlwaysMatchConstraint : IndexedPredicateConstraint
{
    public AlwaysMatchConstraint(IndexedColumnDefinition definition) : base(definition)
    {
    }

    public override bool RowGroupMayMatch(SortedValueArray values)
    {
        return true;
    }

    public override bool TryCountMatches(SortedValueArray values, out int count)
    {
        count = values.Count;
        return true;
    }
}

internal sealed class EqualsConstraint : IndexedPredicateConstraint
{
    private readonly object? _value;

    public EqualsConstraint(IndexedColumnDefinition definition, object? value) : base(definition)
    {
        _value = value;
    }

    public override bool RowGroupMayMatch(SortedValueArray values)
    {
        return values.Contains(_value);
    }

    public override bool TryCountMatches(SortedValueArray values, out int count)
    {
        count = values.CountEquals(_value);
        return true;
    }
}

internal sealed class NotEqualsConstraint : IndexedPredicateConstraint
{
    private readonly object? _value;

    public NotEqualsConstraint(IndexedColumnDefinition definition, object? value) : base(definition)
    {
        _value = value;
    }

    public override bool RowGroupMayMatch(SortedValueArray values)
    {
        return values.HasAnyValueNotEqual(_value);
    }

    public override bool TryCountMatches(SortedValueArray values, out int count)
    {
        count = values.Count - values.CountEquals(_value);
        return true;
    }
}

internal enum ComparisonKind
{
    GreaterThan,
    GreaterThanOrEqual,
    LessThan,
    LessThanOrEqual
}

internal sealed class ComparisonConstraint : IndexedPredicateConstraint
{
    private readonly object? _value;
    private readonly ComparisonKind _kind;

    public ComparisonConstraint(IndexedColumnDefinition definition, object? value, ComparisonKind kind) : base(definition)
    {
        _value = value;
        _kind = kind;
    }

    public override bool RowGroupMayMatch(SortedValueArray values)
    {
        return _kind switch
        {
            ComparisonKind.GreaterThan => values.IntersectsRange(_value, false, null, true),
            ComparisonKind.GreaterThanOrEqual => values.IntersectsRange(_value, true, null, true),
            ComparisonKind.LessThan => values.IntersectsRange(null, true, _value, false),
            ComparisonKind.LessThanOrEqual => values.IntersectsRange(null, true, _value, true),
            _ => true
        };
    }

    public override bool TryCountMatches(SortedValueArray values, out int count)
    {
        count = _kind switch
        {
            ComparisonKind.GreaterThan => values.CountInRange(_value, false, null, true),
            ComparisonKind.GreaterThanOrEqual => values.CountInRange(_value, true, null, true),
            ComparisonKind.LessThan => values.CountInRange(null, true, _value, false),
            ComparisonKind.LessThanOrEqual => values.CountInRange(null, true, _value, true),
            _ => 0
        };
        return true;
    }
}

internal sealed class StartsWithConstraint : IndexedPredicateConstraint
{
    private readonly string _min;
    private readonly string? _maxExclusive;

    private StartsWithConstraint(IndexedColumnDefinition definition, string min, string? maxExclusive)
        : base(definition)
    {
        _min = min;
        _maxExclusive = maxExclusive;
    }

    public static StartsWithConstraint? TryCreate(IndexedColumnDefinition definition, string prefix)
    {
        if (definition.Property.PropertyType != typeof(string))
            return null;

        var max = GetNextPrefix(prefix);
        return new StartsWithConstraint(definition, prefix, max);
    }

    public override bool RowGroupMayMatch(SortedValueArray values)
    {
        return values.IntersectsRange(_min, true, _maxExclusive, false);
    }

    public override bool TryCountMatches(SortedValueArray values, out int count)
    {
        count = values.CountInRange(_min, true, _maxExclusive, false);
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
