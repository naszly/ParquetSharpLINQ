using ParquetSharpLINQ.Models;

namespace ParquetSharpLINQ.Enumeration;

internal static class PartitionFilter
{
    public static IEnumerable<Partition> PrunePartitions(
        IEnumerable<Partition> partitions,
        IReadOnlyDictionary<string, object?> filters)
    {
        return partitions.Where(partition => PartitionMatchesAllFilters(partition, filters));
    }

    private static bool PartitionMatchesAllFilters(
        Partition partition,
        IReadOnlyDictionary<string, object?> filters)
    {
        foreach (var (key, expectedValue) in filters)
        {
            if (partition.Values.TryGetValue(key, out var actualValue))
            {
                if (!ValuesMatch(actualValue, expectedValue))
                {
                    return false;
                }
            }
        }

        return true;
    }

    private static bool ValuesMatch(string partitionValue, object? filterValue)
    {
        if (filterValue == null)
        {
            return string.IsNullOrEmpty(partitionValue);
        }

        if (IsNumeric(filterValue))
        {
            if (long.TryParse(partitionValue, out var partitionNumeric))
            {
                var filterNumeric = Convert.ToInt64(filterValue);
                return partitionNumeric == filterNumeric;
            }
        }

        if (filterValue is DateTime filterDateTime)
        {
            if (DateTime.TryParse(partitionValue, out var partitionDateTime))
            {
                return partitionDateTime == filterDateTime;
            }
        }

        if (filterValue is DateOnly filterDateOnly)
        {
            if (DateOnly.TryParse(partitionValue, out var partitionDateOnly))
            {
                return partitionDateOnly == filterDateOnly;
            }
        }

        var filterString = filterValue.ToString();
        return string.Equals(partitionValue, filterString, StringComparison.OrdinalIgnoreCase);
    }

    private static bool IsNumeric(object value)
    {
        return value is sbyte or byte or short or ushort or int or uint or long or ulong or float or double or decimal;
    }
}

