using ParquetSharp;
using ParquetSharpLINQ.Models;
using ParquetSharpLINQ.Query;

namespace ParquetSharpLINQ.Enumeration;

internal static class StatisticsBasedFilePruner
{
    public static IEnumerable<ParquetFile> PruneFilesByStatistics(
        IEnumerable<ParquetFile> files,
        IReadOnlyDictionary<string, RangeFilter> rangeFilters)
    {
        foreach (var file in files)
        {
            if (file.RowGroups.Count == 0)
            {
                yield return file;
                continue;
            }

            var couldMatch = file.RowGroups
                .Any(rowGroup => RowGroupSatisfiesFilters(rowGroup, rangeFilters));

            if (couldMatch)
            {
                yield return file;
            }
        }
    }

    private static bool RowGroupSatisfiesFilters(
        ParquetRowGroup rowGroup,
        IReadOnlyDictionary<string, RangeFilter> rangeFilters)
    {
        foreach (var (columnName, filter) in rangeFilters)
        {
            if (!filter.HasConstraints)
                continue;

            if (!rowGroup.ColumnStatisticsByPath.TryGetValue(columnName, out var stats))
                continue;

            if (!stats.HasMinMax)
                continue;

            if (!StatisticsOverlapWithFilter(stats, filter))
            {
                return false;
            }
        }

        return true;
    }

    private static bool StatisticsOverlapWithFilter(ParquetColumnStatistics stats, RangeFilter filter)
    {
        if (filter.Min != null && AllValuesAreBelowFilterMinimum(stats, filter))
        {
            return false;
        }

        if (filter.Max != null && AllValuesAreAboveFilterMaximum(stats, filter))
        {
            return false;
        }

        return true;
    }

    private static bool AllValuesAreBelowFilterMinimum(ParquetColumnStatistics stats, RangeFilter filter)
    {
        return CompareStatisticAgainstBoundary(
            stats.MaxRaw, 
            filter.Min!, 
            filter.MinInclusive, 
            isUpperBoundCheck: false,
            stats.PhysicalType, 
            stats.LogicalType);
    }

    private static bool AllValuesAreAboveFilterMaximum(ParquetColumnStatistics stats, RangeFilter filter)
    {
        return CompareStatisticAgainstBoundary(
            stats.MinRaw, 
            filter.Max!, 
            filter.MaxInclusive, 
            isUpperBoundCheck: true,
            stats.PhysicalType, 
            stats.LogicalType);
    }

    private static bool CompareStatisticAgainstBoundary(
        byte[]? statBytes,
        object filterValue,
        bool isInclusive,
        bool isUpperBoundCheck,
        PhysicalType physicalType,
        LogicalType? logicalType)
    {
        var comparison = StatisticComparer.CompareStatisticToFilter(
            statBytes, filterValue, physicalType, logicalType);

        if (!comparison.HasValue)
        {
            return false;
        }

        return isUpperBoundCheck
            ? comparison.Value > 0 || (comparison.Value == 0 && !isInclusive)
            : comparison.Value < 0 || (comparison.Value == 0 && !isInclusive);
    }
}

