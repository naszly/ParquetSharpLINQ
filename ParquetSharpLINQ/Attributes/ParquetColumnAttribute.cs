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
    /// When true, the generated mapper will throw if the column is missing or the value is null for a non-nullable property.
    /// Default: false (use default(TProp)).
    /// </summary>
    public bool ThrowOnMissingOrNull { get; set; } = false;

    /// <summary>
    /// When true, the column can be cached/indexed for predicate optimization.
    /// </summary>
    public bool Indexed { get; set; }

    /// <summary>
    /// Optional comparer type used for indexed columns. Must implement IComparer or IComparer&lt;T&gt;.
    /// </summary>
    public Type? ComparerType { get; set; }
}
