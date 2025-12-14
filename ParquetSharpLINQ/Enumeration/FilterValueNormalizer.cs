namespace ParquetSharpLINQ.Enumeration;

internal static class FilterValueNormalizer
{
    public static object? Normalize(object? value)
    {
        if (value is not string strValue)
        {
            return value;
        }

        if (DateTime.TryParse(strValue, out _) ||
            DateOnly.TryParse(strValue, out _) ||
            long.TryParse(strValue, out _))
        {
            return strValue;
        }

        return strValue.ToLowerInvariant();
    }

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

