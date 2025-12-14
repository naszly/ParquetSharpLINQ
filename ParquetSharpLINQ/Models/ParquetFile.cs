namespace ParquetSharpLINQ.Models;

/// <summary>
/// Information about a single Parquet file discovered in a partition.
/// Contains the blob/file path and optional metadata discovered at discovery time.
/// </summary>
public sealed class ParquetFile
{
    /// <summary>
    /// Path to the parquet file. For filesystem partitions this will be an absolute file path;
    /// for blob storage this will be the blob name relative to the container.
    /// </summary>
    public string Path { get; init; } = string.Empty;

    /// <summary>File size in bytes if available.</summary>
    public long? SizeBytes { get; init; }

    /// <summary>Last modified timestamp if available.</summary>
    public DateTimeOffset? LastModified { get; init; }

    /// <summary>
    /// Total number of rows in the file if available.
    /// </summary>
    public long? RowCount { get; init; }

    /// <summary>
    /// Row-group level metadata and statistics discovered or later populated.
    /// </summary>
    public IReadOnlyList<ParquetRowGroup> RowGroups { get; init; } = Array.Empty<ParquetRowGroup>();

    public override string ToString() => Path;
}