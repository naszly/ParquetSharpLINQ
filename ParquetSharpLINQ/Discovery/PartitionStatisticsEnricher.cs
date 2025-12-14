using System.Collections.Immutable;
using ParquetSharpLINQ.Interfaces;
using ParquetSharpLINQ.Models;

namespace ParquetSharpLINQ.Discovery;

/// <summary>
/// Helper class for enriching partitions with statistics in parallel.
/// Encapsulates the common logic for calling statistics providers across multiple files.
/// </summary>
public sealed class PartitionStatisticsEnricher
{
    private readonly IParquetStatisticsProvider _statisticsProvider;
    private readonly int _parallelism;

    /// <summary>
    /// Creates a new partition statistics enricher.
    /// </summary>
    /// <param name="statisticsProvider">The statistics provider to use for enrichment</param>
    /// <param name="parallelism">Maximum degree of parallelism (default: CPU count)</param>
    public PartitionStatisticsEnricher(
        IParquetStatisticsProvider statisticsProvider,
        int parallelism = 0)
    {
        _statisticsProvider = statisticsProvider ?? throw new ArgumentNullException(nameof(statisticsProvider));
        _parallelism = parallelism <= 0 ? Environment.ProcessorCount : parallelism;
    }

    /// <summary>
    /// Enriches partitions with statistics by calling the statistics provider in parallel.
    /// </summary>
    /// <param name="partitions">The partitions to enrich</param>
    /// <returns>Partitions with enriched file metadata</returns>
    public IEnumerable<Partition> Enrich(IEnumerable<Partition> partitions)
    {
        var partitionsList = partitions.ToList();
        
        // Flatten all files with their partition reference
        var allFiles = partitionsList
            .SelectMany(p => p.Files.Select(f => (Partition: p, File: f)))
            .ToList();

        if (allFiles.Count == 0)
        {
            return partitionsList;
        }

        var enriched = new ParquetFile[allFiles.Count];

        // Enrich files in parallel - blocks synchronously as required by IPartitionDiscoveryStrategy interface
        Parallel.ForEachAsync(
            Enumerable.Range(0, allFiles.Count),
            new ParallelOptions
            {
                MaxDegreeOfParallelism = _parallelism
            },
            async (i, ct) =>
            {
                var (_, file) = allFiles[i];
                enriched[i] = await _statisticsProvider.EnrichAsync(file, ct).ConfigureAwait(false);
            }).ConfigureAwait(false).GetAwaiter().GetResult();

        // Group enriched files back by partition path
        var byPartitionPath = allFiles
            .Select((entry, index) => new { entry.Partition.Path, File = enriched[index] })
            .GroupBy(x => x.Path, StringComparer.OrdinalIgnoreCase)
            .ToDictionary(
                g => g.Key,
                g => g.Select(x => x.File).ToImmutableArray(),
                StringComparer.OrdinalIgnoreCase);

        // Reconstruct partitions with enriched files
        return partitionsList.Select(p => new Partition
        {
            Path = p.Path,
            Values = p.Values,
            Files = byPartitionPath.TryGetValue(p.Path, out var enrichedFiles)
                ? enrichedFiles
                : p.Files
        }).ToImmutableArray();
    }
}

