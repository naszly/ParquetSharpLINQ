using ParquetSharpLINQ.Constants;
using ParquetSharpLINQ.Models;
using ParquetSharpLINQ.ParquetSharp;

namespace ParquetSharpLINQ.Enumeration;

internal static class PartitionRowEnricher
{
    public static ParquetRow EnrichWithPartitionValues(
        ParquetRow row,
        IReadOnlyDictionary<string, string> partitionValues)
    {
        if (partitionValues.Count == 0)
        {
            return row;
        }

        var (enrichedColumnNames, enrichedValues) = CreateEnrichedArrays(row, partitionValues);
        CopyExistingRowData(row, enrichedColumnNames, enrichedValues);
        AppendPartitionData(partitionValues, enrichedColumnNames, enrichedValues, row.ColumnCount);

        return new ParquetRow(enrichedColumnNames, enrichedValues);
    }

    public static ParquetRow CreateRowFromPartitionMetadata(Partition partition)
    {
        var columnNames = new string[partition.Values.Count];
        var values = new object?[partition.Values.Count];
        var index = 0;

        foreach (var (key, value) in partition.Values)
        {
            columnNames[index] = $"{PartitionConstants.PartitionPrefix}{key}";
            values[index] = FilterValueNormalizer.NormalizePartitionValue(value);
            index++;
        }

        return new ParquetRow(columnNames, values);
    }

    private static (string[] columnNames, object?[] values) CreateEnrichedArrays(
        ParquetRow row,
        IReadOnlyDictionary<string, string> partitionValues)
    {
        var totalColumns = row.ColumnCount + partitionValues.Count;
        return (new string[totalColumns], new object?[totalColumns]);
    }

    private static void CopyExistingRowData(ParquetRow row, string[] targetColumnNames, object?[] targetValues)
    {
        var sourceColumnNames = row.ColumnNames;
        var sourceValues = row.Values;
        
        for (var i = 0; i < row.ColumnCount; i++)
        {
            targetColumnNames[i] = sourceColumnNames[i];
            targetValues[i] = sourceValues[i];
        }
    }

    private static void AppendPartitionData(
        IReadOnlyDictionary<string, string> partitionValues,
        string[] targetColumnNames,
        object?[] targetValues,
        int offset)
    {
        var index = 0;
        foreach (var (key, value) in partitionValues)
        {
            var partitionKey = $"{PartitionConstants.PartitionPrefix}{key}";
            targetColumnNames[offset + index] = partitionKey;
            targetValues[offset + index] = FilterValueNormalizer.NormalizePartitionValue(value);
            index++;
        }
    }
}

