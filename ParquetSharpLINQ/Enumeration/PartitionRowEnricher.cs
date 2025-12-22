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

        var (enrichedColumnNames, enrichedBuffers) = CreateEnrichedArrays(row, partitionValues);
        CopyExistingRowData(row, enrichedColumnNames, enrichedBuffers);
        AppendPartitionData(partitionValues, enrichedColumnNames, enrichedBuffers, row.ColumnCount);

        return new ParquetRow(enrichedColumnNames, enrichedBuffers, row.RowIndex);
    }

    public static ParquetRow CreateRowFromPartitionMetadata(Partition partition)
    {
        var columnNames = new string[partition.Values.Count];
        var buffers = new IColumnBuffer[partition.Values.Count];
        var index = 0;

        foreach (var (key, value) in partition.Values)
        {
            columnNames[index] = $"{PartitionConstants.PartitionPrefix}{key}";
            buffers[index] = new ConstantColumnBuffer<string>(FilterValueNormalizer.NormalizePartitionValue(value));
            index++;
        }

        return new ParquetRow(columnNames, buffers, 0);
    }

    private static (string[] columnNames, IColumnBuffer[] buffers) CreateEnrichedArrays(
        ParquetRow row,
        IReadOnlyDictionary<string, string> partitionValues)
    {
        var totalColumns = row.ColumnCount + partitionValues.Count;
        return (new string[totalColumns], new IColumnBuffer[totalColumns]);
    }

    private static void CopyExistingRowData(ParquetRow row, string[] targetColumnNames, IColumnBuffer[] targetBuffers)
    {
        for (var i = 0; i < row.ColumnCount; i++)
        {
            targetColumnNames[i] = row.ColumnNames[i];
            targetBuffers[i] = row.Buffers[i];
        }
    }

    private static void AppendPartitionData(
        IReadOnlyDictionary<string, string> partitionValues,
        string[] targetColumnNames,
        IColumnBuffer[] targetBuffers,
        int offset)
    {
        var index = 0;
        foreach (var (key, value) in partitionValues)
        {
            var partitionKey = $"{PartitionConstants.PartitionPrefix}{key}";
            targetColumnNames[offset + index] = partitionKey;
            targetBuffers[offset + index] =
                new ConstantColumnBuffer<string>(FilterValueNormalizer.NormalizePartitionValue(value));
            index++;
        }
    }
}
