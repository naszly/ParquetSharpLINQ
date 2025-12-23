using System.Runtime.CompilerServices;
using ParquetSharpLINQ.ParquetSharp.ParquetRow;

namespace ParquetSharpLINQ.Mappers;

/// <summary>
/// Shared helper methods for generated ParquetMapper classes.
/// Contains common conversion and lookup logic to avoid code duplication.
/// </summary>
public static class ParquetMapperHelpers
{
    /// <summary>
    /// Tries to get a typed value from the ParquetRow with case-insensitive key matching.
    /// </summary>
    public static bool TryGetValue<T>(ParquetRow row, string key, out T? value)
    {
        var columnNames = row.ColumnNames;
        for (var i = 0; i < columnNames.Length; i++)
        {
            if (!string.Equals(columnNames[i], key, StringComparison.OrdinalIgnoreCase))
                continue;

            value = row.GetValue<T>(i);
            return true;
        }

        Unsafe.SkipInit(out value);
        return false;
    }
}
