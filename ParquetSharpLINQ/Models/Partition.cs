namespace ParquetSharpLINQ.Models;

public sealed class Partition
{
    /// <summary>
    ///     Absolute directory path containing parquet files for this partition (leaf directory).
    /// </summary>
    public string Path { get; init; } = string.Empty;

    /// <summary>
    ///     Hive-style partition key -> value pairs (e.g. year -> 2024, country -> US).
    /// </summary>
    public IReadOnlyDictionary<string, string> Values { get; init; } = new Dictionary<string, string>();

    /// <summary>
    ///     Optional: Explicit list of files to read (used by Delta Lake).
    ///     If null, all parquet files in Path will be enumerated (Hive-style).
    /// </summary>
    public IReadOnlyList<string>? Files { get; init; }
}