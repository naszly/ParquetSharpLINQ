using System;
using System.Collections.Generic;
using System.IO;

namespace ParquetSharpLINQ.Common;

/// <summary>
/// Utility for parsing Hive-style partition paths (e.g., "year=2024/month=06").
/// </summary>
public static class HivePartitionParser
{
    private const string ParquetExtension = ".parquet";

    /// <summary>
    /// Checks if a file name is a Parquet file (case-insensitive).
    /// </summary>
    public static bool IsParquetFile(string fileName)
    {
        return fileName.EndsWith(ParquetExtension, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// Parses partition key-value pairs from a path string.
    /// </summary>
    /// <param name="path">Path containing partition segments like "year=2024/month=06"</param>
    /// <returns>Dictionary of partition keys and values</returns>
    public static Dictionary<string, string> ParsePartitionValues(string path)
    {
        var values = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        
        if (!string.IsNullOrEmpty(path) && path != ".")
        {
            ParsePartitionValues(path.AsSpan(), values);
        }
        
        return values;
    }

    /// <summary>
    /// Parses partition key-value pairs from a path span.
    /// </summary>
    /// <param name="relativePath">Path span containing partition segments</param>
    /// <param name="values">Dictionary to populate with partition values</param>
    private static void ParsePartitionValues(ReadOnlySpan<char> relativePath, Dictionary<string, string> values)
    {
        var remaining = relativePath;

        while (remaining.Length > 0)
        {
            var separatorIndex = remaining.IndexOfAny(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar);
            var part = separatorIndex >= 0 ? remaining[..separatorIndex] : remaining;

            if (part.Length > 0)
            {
                ParsePartitionKeyValue(part, values);
            }

            if (separatorIndex < 0)
            {
                break;
            }

            remaining = remaining[(separatorIndex + 1)..];
        }
    }

    private static void ParsePartitionKeyValue(ReadOnlySpan<char> part, Dictionary<string, string> values)
    {
        var eqIndex = part.IndexOf('=');
        if (eqIndex > 0 && eqIndex < part.Length - 1)
        {
            var key = part[..eqIndex].ToString();
            var value = part[(eqIndex + 1)..].ToString();
            values[key] = value;
        }
    }
}
