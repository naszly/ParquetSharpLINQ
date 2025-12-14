using System.Collections.ObjectModel;
using ParquetSharp;
using ParquetSharpLINQ.Models;

namespace ParquetSharpLINQ.Statistics;

/// <summary>
/// Extracts Parquet statistics from streams.
/// This class encapsulates the logic for reading Parquet metadata and statistics from any stream.
/// </summary>
public sealed class ParquetStatisticsExtractor
{
    /// <summary>
    /// Extracts statistics from a stream containing a Parquet file.
    /// </summary>
    /// <param name="stream">The stream to read from</param>
    /// <param name="originalFile">The original file metadata</param>
    /// <param name="sizeBytes">Optional file size in bytes</param>
    /// <param name="lastModified">Optional last modified timestamp</param>
    /// <returns>Enriched ParquetFile with statistics</returns>
    public ParquetFile ExtractFromStream(
        Stream stream,
        ParquetFile originalFile,
        long? sizeBytes = null,
        DateTimeOffset? lastModified = null)
    {
        ArgumentNullException.ThrowIfNull(stream);
        ArgumentNullException.ThrowIfNull(originalFile);
        
        if (!stream.CanSeek)
        {
            throw new NotSupportedException(
                "Statistics extraction requires a seekable stream because Parquet metadata is located at the end of the file.");
        }

        stream.Position = 0;

        using var reader = new ParquetFileReader(stream);
        return ExtractStatistics(originalFile, reader, sizeBytes, lastModified);
    }

    /// <summary>
    /// Extracts statistics from a ParquetFileReader.
    /// </summary>
    private static ParquetFile ExtractStatistics(
        ParquetFile originalFile,
        ParquetFileReader reader,
        long? sizeBytes,
        DateTimeOffset? lastModified)
    {
        var meta = reader.FileMetaData;
        var rowGroups = new List<ParquetRowGroup>(meta.NumRowGroups);

        for (int rg = 0; rg < meta.NumRowGroups; rg++)
        {
            using var rowGroupReader = reader.RowGroup(rg);
            var rgMeta = rowGroupReader.MetaData;
            var statsByPath = new Dictionary<string, ParquetColumnStatistics>(StringComparer.OrdinalIgnoreCase);

            int numLeafColumns = meta.Schema.NumColumns;
            for (int c = 0; c < numLeafColumns; c++)
            {
                var colMeta = rgMeta.GetColumnChunkMetaData(c);
                var colDesc = meta.Schema.Column(c);

                var stats = ExtractColumnStatistics(colMeta, colDesc);
                if (stats != null)
                    statsByPath[stats.ColumnPath] = stats;
            }

            rowGroups.Add(new ParquetRowGroup
            {
                Index = rg,
                NumRows = rgMeta.NumRows,
                TotalByteSize = rgMeta.TotalByteSize,
                ColumnStatisticsByPath =
                    new ReadOnlyDictionary<string, ParquetColumnStatistics>(statsByPath)
            });
        }

        return new ParquetFile
        {
            Path = originalFile.Path,
            SizeBytes = sizeBytes ?? originalFile.SizeBytes,
            LastModified = lastModified ?? originalFile.LastModified,
            RowCount = meta.NumRows,
            RowGroups = rowGroups
        };
    }

    /// <summary>
    /// Extracts column statistics from a column chunk metadata.
    /// </summary>
    private static ParquetColumnStatistics? ExtractColumnStatistics(
        ColumnChunkMetaData chunk,
        ColumnDescriptor descriptor)
    {
        if (!chunk.IsStatsSet)
            return null;

        var s = chunk.Statistics;
        if (s == null || !s.HasMinMax)
            return null;

        var physicalType = descriptor.PhysicalType;
        var minRaw = ConvertToBytes(s.MinUntyped);
        var maxRaw = ConvertToBytes(s.MaxUntyped);

        return new ParquetColumnStatistics
        {
            ColumnPath = descriptor.Path.ToDotString(),
            PhysicalType = physicalType,
            LogicalType = descriptor.LogicalType,
            MinRaw = minRaw,
            MaxRaw = maxRaw,
            NullCount = s.NullCount,
            DistinctCount = s.DistinctCount
        };
    }

    /// <summary>
    /// Converts various types to byte arrays for storage in statistics.
    /// </summary>
    private static byte[]? ConvertToBytes(object? value)
    {
        return value switch
        {
            null => null,
            byte[] b => b,
            string s => System.Text.Encoding.UTF8.GetBytes(s),
            int i => BitConverter.GetBytes(i),
            long l => BitConverter.GetBytes(l),
            float f => BitConverter.GetBytes(f),
            double d => BitConverter.GetBytes(d),
            bool bo => BitConverter.GetBytes(bo),
            _ => null // Keep conservative for unknown types
        };
    }
}

