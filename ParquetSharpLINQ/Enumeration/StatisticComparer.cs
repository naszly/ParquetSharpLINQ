using ParquetSharp;
using ParquetSharpLINQ.Models;

namespace ParquetSharpLINQ.Enumeration;

internal static class StatisticComparer
{
    public static int? CompareStatisticToFilter(
        byte[]? statBytes, 
        object filterValue, 
        PhysicalType physicalType, 
        LogicalType? logicalType)
    {
        if (statBytes == null)
        {
            return null;
        }

        try
        {
            return filterValue switch
            {
                DateOnly dateOnly => CompareWithStatBytes(statBytes, dateOnly, physicalType, logicalType),
                DateTime dateTime => CompareWithStatBytes(statBytes, dateTime, physicalType, logicalType),
                string str => CompareWithStatBytes(statBytes, str, physicalType, logicalType),
                int intValue => CompareWithStatBytes(statBytes, intValue, physicalType, logicalType),
                long longValue => CompareWithStatBytes(statBytes, longValue, physicalType, logicalType),
                short shortValue => CompareWithStatBytes(statBytes, (int)shortValue, physicalType, logicalType),
                byte byteValue => CompareWithStatBytes(statBytes, (int)byteValue, physicalType, logicalType),
                float floatValue => CompareWithStatBytes(statBytes, floatValue, physicalType, logicalType),
                double doubleValue => CompareWithStatBytes(statBytes, doubleValue, physicalType, logicalType),
                _ => null
            };
        }
        catch
        {
            return null;
        }
    }

    private static int? CompareWithStatBytes<TValue>(
        byte[] statBytes, 
        TValue filterValue, 
        PhysicalType physicalType, 
        LogicalType? logicalType) 
        where TValue : IComparable<TValue>
    {
        var statValue = ParquetColumnStatistics.DecodeStatisticValue<TValue>(
            statBytes, 
            physicalType, 
            logicalType);

        if (statValue == null)
        {
            return null;
        }

        return statValue.CompareTo(filterValue);
    }
}