namespace ParquetSharpLINQ;

/// <summary>
/// Shared helper methods for generated ParquetMapper classes.
/// Contains common conversion and lookup logic to avoid code duplication.
/// </summary>
public static class ParquetMapperHelpers
{
    /// <summary>
    /// Tries to get a value from the row dictionary with case-insensitive key matching.
    /// </summary>
    public static bool TryGetValue(IReadOnlyDictionary<string, object?> row, string key, out object? value)
    {
        if (row.TryGetValue(key, out value))
            return true;

        foreach (var pair in row)
        {
            if (string.Equals(pair.Key, key, StringComparison.OrdinalIgnoreCase))
            {
                value = pair.Value;
                return true;
            }
        }

        value = null;
        return false;
    }

    /// <summary>
    /// Converts a value from the Parquet row to the target type T.
    /// Handles ParquetSharp-specific types and common conversions.
    /// </summary>
    public static T ConvertValue<T>(object? value)
    {
        if (value == null)
            return default!;

        if (value is T typed)
            return typed;

        var targetType = typeof(T);
        var underlyingType = Nullable.GetUnderlyingType(targetType) ?? targetType;

        if (underlyingType.IsEnum)
        {
            if (value is string enumText)
                return (T)Enum.Parse(underlyingType, enumText, true);
            return (T)Enum.ToObject(underlyingType, value);
        }

        // Handle ParquetSharp.Date conversions
        if (value is global::ParquetSharp.Date parquetDate)
        {
            var dateTime = parquetDate.DateTime;

            if (underlyingType == typeof(DateTime))
                return (T)(object)dateTime;

#if NET6_0_OR_GREATER
            if (underlyingType == typeof(DateOnly))
                return (T)(object)DateOnly.FromDateTime(dateTime);
#endif
        }

#if NET6_0_OR_GREATER
        if (underlyingType == typeof(DateOnly) && value is string dateOnlyStr)
            return (T)(object)DateOnly.Parse(dateOnlyStr, System.Globalization.CultureInfo.InvariantCulture);

        if (underlyingType == typeof(TimeOnly) && value is string timeOnlyStr)
            return (T)(object)TimeOnly.Parse(timeOnlyStr, System.Globalization.CultureInfo.InvariantCulture);
#endif

        return (T)Convert.ChangeType(value, underlyingType, System.Globalization.CultureInfo.InvariantCulture);
    }
}
