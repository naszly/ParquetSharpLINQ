namespace ParquetSharpLINQ.Query;

/// <summary>
/// Represents a range constraint for statistics-based pruning.
/// Extracted from WHERE clause predicates like x &gt;= 10 AND x &lt; 100.
/// </summary>
public sealed class RangeFilter
{
    /// <summary>
    /// Minimum value constraint (&gt;=). Null means no lower bound.
    /// </summary>
    public object? Min { get; set; }

    /// <summary>
    /// Maximum value constraint (&lt;=). Null means no upper bound.
    /// </summary>
    public object? Max { get; set; }

    /// <summary>
    /// Whether the min constraint is inclusive (&gt;=) or exclusive (&gt;).
    /// </summary>
    public bool MinInclusive { get; set; } = true;

    /// <summary>
    /// Whether the max constraint is inclusive (&lt;=) or exclusive (&lt;).
    /// </summary>
    public bool MaxInclusive { get; set; } = true;

    /// <summary>
    /// True if this filter has at least one constraint (min or max).
    /// </summary>
    public bool HasConstraints => Min != null || Max != null;
}