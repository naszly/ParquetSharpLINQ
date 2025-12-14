using ParquetSharp;

namespace ParquetSharpLINQ.Models;

/// <summary>
/// Column-level statistics for a Parquet file or row-group.
/// Min/Max are stored as raw bytes (as they appear in Parquet statistics), plus type info.
/// Consumers can compare raw values safely (e.g., ordinal UTF-8 for strings) or decode when needed.
/// </summary>
public sealed class ParquetColumnStatistics
{
    /// <summary>
    /// Full column path (for nested columns), e.g. "customer.name" or "root.customer.name".
    /// </summary>
    public string ColumnPath { get; init; } = string.Empty;

    /// <summary>
    /// Physical type of the column (Parquet primitive type).
    /// </summary>
    public PhysicalType PhysicalType { get; init; }

    /// <summary>
    /// Logical type (optional). Helps interpret raw stats correctly (e.g., Decimal, Timestamp, String).
    /// </summary>
    public LogicalType? LogicalType { get; init; }

    /// <summary>
    /// Minimum value as raw bytes (as reported by Parquet metadata). Null when not available.
    /// </summary>
    public byte[]? MinRaw { get; init; }

    /// <summary>
    /// Maximum value as raw bytes (as reported by Parquet metadata). Null when not available.
    /// </summary>
    public byte[]? MaxRaw { get; init; }

    /// <summary>
    /// True if both MinRaw and MaxRaw are present.
    /// </summary>
    public bool HasMinMax => MinRaw != null && MaxRaw != null;

    /// <summary>
    /// Number of null values in this column. Null when not available.
    /// </summary>
    public long? NullCount { get; init; }

    /// <summary>
    /// Number of distinct values in this column. Null when not available.
    /// </summary>
    public long? DistinctCount { get; init; }

    public override string ToString() => ColumnPath;

    public bool TryGetMinMax<T>(out T? min, out T? max)
    {
        min = default;
        max = default;

        if (MinRaw == null || MaxRaw == null)
            return false;

        try
        {
            var minObj = DecodeValue(MinRaw, typeof(T));
            var maxObj = DecodeValue(MaxRaw, typeof(T));

            if (minObj is null || maxObj is null)
                return false;

            min = (T)minObj;
            max = (T)maxObj;
            return true;
        }
        catch
        {
            min = default;
            max = default;
            return false;
        }
    }

    private object? DecodeValue(byte[] raw, Type targetType)
    {
        return targetType switch
        {
            // STRING
            _ when targetType == typeof(string) &&
                   (PhysicalType == PhysicalType.ByteArray ||
                    PhysicalType == PhysicalType.FixedLenByteArray)
                => System.Text.Encoding.UTF8.GetString(raw),

            // DATE (Parquet DATE -> DateOnly)
            _ when targetType == typeof(DateOnly) &&
                   LogicalType is DateLogicalType &&
                   PhysicalType == PhysicalType.Int32 &&
                   raw.Length >= 4
                => DecodeDateOnly(raw),

            // INT32
            _ when targetType == typeof(int) && raw.Length >= 4
                => BitConverter.ToInt32(raw, 0),

            // INT64
            _ when targetType == typeof(long) && raw.Length >= 8
                => BitConverter.ToInt64(raw, 0),

            // FLOAT
            _ when targetType == typeof(float) && raw.Length >= 4
                => BitConverter.ToSingle(raw, 0),

            // DOUBLE
            _ when targetType == typeof(double) && raw.Length >= 8
                => BitConverter.ToDouble(raw, 0),

            // BOOL
            _ when targetType == typeof(bool) && raw.Length >= 1
                => BitConverter.ToBoolean(raw, 0),

            _ => null
        };
    }

    private static DateOnly DecodeDateOnly(byte[] raw)
    {
        // Parquet DATE: INT32 days since 1970-01-01 (Unix epoch)
        var daysSinceEpoch = BitConverter.ToInt32(raw, 0);

        // Create DateOnly from Unix epoch and add days
        var epoch = new DateOnly(1970, 1, 1);
        return epoch.AddDays(daysSinceEpoch);
    }
}