using System.Collections.ObjectModel;

namespace ParquetSharpLINQ.Models;

/// <summary>
/// Information about a single Parquet row-group within a Parquet file.
/// Contains row-group index, row count, byte size and optional per-column statistics.
/// </summary>
public sealed class ParquetRowGroup
{
    /// <summary>Zero-based index of the row group within the file.</summary>
    public int Index { get; init; }

    /// <summary>Number of rows in this row group when available.</summary>
    public long? NumRows { get; init; }

    /// <summary>
    /// Total row-group byte size as reported by metadata (commonly uncompressed total size in Parquet).
    /// Kept optional because availability/meaning can vary depending on the reader/writer.
    /// </summary>
    public long? TotalByteSize { get; init; }

    /// <summary>
    /// Column statistics indexed by ColumnPath for fast lookup.
    /// </summary>
    public IReadOnlyDictionary<string, ParquetColumnStatistics> ColumnStatisticsByPath { get; init; }
        = new ReadOnlyDictionary<string, ParquetColumnStatistics>(new Dictionary<string, ParquetColumnStatistics>(StringComparer.OrdinalIgnoreCase));

    public override string ToString() => $"RowGroup {Index}";
}