namespace ParquetSharpLINQ.Enumeration;

internal static class FilterValueNormalizer
{
    public static string NormalizePartitionValue(string value)
    {
        if (DateTime.TryParse(value, out _) ||
            DateOnly.TryParse(value, out _) ||
            long.TryParse(value, out _))
        {
            return value;
        }

        return value.ToLowerInvariant();
    }
}

