using System.Collections.Immutable;
using Azure.Storage.Blobs;
using ParquetSharpLINQ.Discovery;
using ParquetSharpLINQ.Interfaces;
using ParquetSharpLINQ.Models;

namespace ParquetSharpLINQ.Azure;

/// <summary>
/// Azure Blob Storage-based partition discovery strategy.
/// Supports both Delta Lake and Hive-style partitioning.
/// Automatically detects Delta tables by checking for _delta_log/ prefix.
/// </summary>
public class AzureBlobPartitionDiscovery : IPartitionDiscoveryStrategy
{
    private readonly BlobContainerClient _containerClient;
    private readonly string _blobPrefix;
    private readonly Lazy<AzureDeltaLogReader> _deltaLogReader;
    private readonly PartitionStatisticsEnricher? _statisticsEnricher;

    /// <summary>
    /// Creates a new Azure Blob Storage partition discovery strategy.
    /// </summary>
    /// <param name="containerClient">Azure Blob Container client</param>
    /// <param name="blobPrefix">Optional blob prefix/subfolder path (e.g., "data/sales/" or empty for root)</param>
    /// <param name="cacheExpiration">Optional cache expiration duration for Delta log (default: 5 minutes)</param>
    /// <param name="statisticsProvider">Optional statistics provider for enriching file metadata with row-group statistics</param>
    /// <param name="statisticsParallelism">Max degree of parallelism for statistics enrichment (default: CPU count)</param>
    public AzureBlobPartitionDiscovery(
        BlobContainerClient containerClient,
        string blobPrefix = "",
        TimeSpan? cacheExpiration = null,
        IParquetStatisticsProvider? statisticsProvider = null,
        int statisticsParallelism = 0)
    {
        _containerClient = containerClient ?? throw new ArgumentNullException(nameof(containerClient));
        _blobPrefix = NormalizePrefix(blobPrefix);
        _deltaLogReader = new Lazy<AzureDeltaLogReader>(() => 
            new AzureDeltaLogReader(_containerClient, _blobPrefix, cacheExpiration));
        _statisticsEnricher = statisticsProvider != null 
            ? new PartitionStatisticsEnricher(statisticsProvider, statisticsParallelism)
            : null;
    }

    public IEnumerable<Partition> DiscoverPartitions()
    {
        var partitions = IsDeltaTable() ? DiscoverFromDeltaLog() : DiscoverFromBlobs();
        
        // Enrich with statistics if provider is configured
        if (_statisticsEnricher != null)
        {
            partitions = _statisticsEnricher.Enrich(partitions);
        }

        return partitions;
    }

    public void ClearDeltaLogCache()
    {
        if (_deltaLogReader.IsValueCreated)
        {
            _deltaLogReader.Value.ClearCache();
        }
    }

    /// <summary>
    /// Normalizes a blob prefix by ensuring it doesn't start with / and ends with / (if non-empty).
    /// </summary>
    private static string NormalizePrefix(string prefix)
    {
        if (string.IsNullOrWhiteSpace(prefix))
        {
            return string.Empty;
        }

        prefix = prefix.Trim();
        
        if (prefix.StartsWith('/'))
        {
            prefix = prefix.TrimStart('/');
        }

        if (!prefix.EndsWith('/'))
        {
            prefix += '/';
        }

        return prefix;
    }

    private bool IsDeltaTable()
    {
        var deltaLogPrefix = _blobPrefix + "_delta_log/";
        var deltaLogBlobs = _containerClient.GetBlobs(prefix: deltaLogPrefix);
        return deltaLogBlobs.Any();
    }

    private IEnumerable<Partition> DiscoverFromDeltaLog()
    {
        var snapshot = _deltaLogReader.Value.GetLatestSnapshot();
        var partitionGroups = new Dictionary<string, (Dictionary<string, string> Values, List<string> Files)>(StringComparer.OrdinalIgnoreCase);

        foreach (var addAction in snapshot.ActiveFiles)
        {
            var fullFilePath = string.IsNullOrEmpty(_blobPrefix) 
                ? addAction.Path 
                : _blobPrefix + addAction.Path;
            
            var directory = GetDirectory(fullFilePath);

            if (string.IsNullOrEmpty(directory))
            {
                directory = _blobPrefix.TrimEnd('/');
            }

            if (!partitionGroups.ContainsKey(directory))
            {
                var partitionValues = addAction.PartitionValues ?? new Dictionary<string, string>();
                partitionGroups[directory] = (
                    new Dictionary<string, string>(partitionValues, StringComparer.OrdinalIgnoreCase),
                    []
                );
            }

            partitionGroups[directory].Files.Add(fullFilePath);
        }

        return partitionGroups.Select(kvp => new Partition
        {
            Path = kvp.Key,
            Values = kvp.Value.Values,
            Files = kvp.Value.Files.Select(f => new ParquetFile { Path = f }).ToList()
        });
    }

    private IEnumerable<Partition> DiscoverFromBlobs()
    {
        var blobs = _containerClient.GetBlobs(prefix: _blobPrefix);
        var partitionPaths = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var files = new List<string>();

        foreach (var blob in blobs)
        {
            if (!HivePartitionParser.IsParquetFile(blob.Name))
            {
                continue;
            }
            
            files.Add(blob.Name);

            var directory = GetDirectory(blob.Name);
            if (!string.IsNullOrEmpty(directory))
            {
                partitionPaths.Add(directory);
            }
        }

        foreach (var path in partitionPaths.OrderBy(p => p))
        {
            var relativePath = path;
            if (!string.IsNullOrEmpty(_blobPrefix) && relativePath.StartsWith(_blobPrefix, StringComparison.OrdinalIgnoreCase))
            {
                relativePath = relativePath.Substring(_blobPrefix.Length);
            }
            
            var filesArray = files
                .Where(f => GetDirectory(f).Equals(path, StringComparison.OrdinalIgnoreCase))
                .Select(f => new ParquetFile { Path = f })
                .ToImmutableArray();
            
            var values = HivePartitionParser.ParsePartitionValues(relativePath);
            yield return new Partition { Path = path, Values = values, Files = filesArray};
        }
    }

    private static string GetDirectory(string blobName)
    {
        var lastSlash = blobName.LastIndexOf('/');
        return lastSlash > 0 ? blobName[..lastSlash] : "";
    }
}

