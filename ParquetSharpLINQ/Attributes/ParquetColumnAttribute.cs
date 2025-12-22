namespace ParquetSharpLINQ.Attributes;

[AttributeUsage(AttributeTargets.Property)]
public sealed class ParquetColumnAttribute : Attribute
{
    public ParquetColumnAttribute()
    {
    }

    public ParquetColumnAttribute(string name)
    {
        Name = name;
    }

    /// <summary>
    /// Name of the column in the parquet file (or partition key). If null, the property name is used.
    /// </summary>
    public string? Name { get; set; }

    /// <summary>
    /// When true, indicates the value should be taken from the partition values
    /// </summary>
    public bool IsPartition { get; set; }

    /// <summary>
    /// When true, missing columns are allowed and default(TProp) is used.
    /// Only honored for nullable properties.
    /// Default: false (missing columns throw).
    /// </summary>
    public bool AllowMissing { get; set; } = false;

    /// <summary>
    /// When true, the column can be cached/indexed for predicate optimization.
    /// </summary>
    public bool Indexed { get; set; }

    /// <summary>
    /// Optional comparer type used for indexed columns. Must implement IComparer or IComparer&lt;T&gt;.
    /// </summary>
    public Type? ComparerType { get; set; }
}
