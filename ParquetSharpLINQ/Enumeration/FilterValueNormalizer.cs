namespace ParquetSharpLINQ.Enumeration;

internal static class FilterValueNormalizer
{
    public static string NormalizePartitionValue(string value)
    {
        return value.ToLowerInvariant();
    }
}

