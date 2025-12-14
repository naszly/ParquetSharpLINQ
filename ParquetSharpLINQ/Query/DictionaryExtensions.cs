namespace ParquetSharpLINQ.Query;

/// <summary>
/// Extension methods for Dictionary operations.
/// </summary>
internal static class DictionaryExtensions
{
    /// <summary>
    /// Gets the value associated with the specified key, or adds a new value if the key doesn't exist.
    /// </summary>
    public static TValue GetOrAdd<TKey, TValue>(
        this Dictionary<TKey, TValue> dictionary, 
        TKey key, 
        Func<TKey, TValue> valueFactory) 
        where TKey : notnull
    {
        if (dictionary.TryGetValue(key, out var value))
        {
            return value;
        }
        value = valueFactory(key);
        dictionary[key] = value;
        return value;
    }
}

