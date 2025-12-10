using Azure.Storage.Blobs;
using ParquetSharpLINQ.Models;

namespace ParquetSharpLINQ.Azure;

/// <summary>
/// Partition discovery for Azure Blob Storage that supports both Delta Lake and Hive-style partitioning.
/// Automatically detects Delta tables by checking for _delta_log/ prefix.
/// </summary>
public static class AzurePartitionDiscovery
{
    /// <summary>
    /// Discovers partitions from Azure Blob Storage container.
    /// If _delta_log/ blobs exist, reads from Delta transaction log.
    /// Otherwise, scans blobs for Parquet files (Hive-style).
    /// </summary>
    /// <param name="containerClient">Azure Blob Container client</param>
    /// <param name="deltaLogReader">Reusable Delta log reader for caching</param>
    /// <param name="blobPrefix">Optional blob prefix to limit discovery to a subfolder</param>
    /// <returns>Enumerable of discovered partitions</returns>
    public static IEnumerable<Partition> Discover(
        BlobContainerClient containerClient,
        Lazy<AzureDeltaLogReader> deltaLogReader,
        string blobPrefix = "")
    {
        ArgumentNullException.ThrowIfNull(containerClient);

        blobPrefix = NormalizePrefix(blobPrefix);

        if (IsDeltaTable(containerClient, blobPrefix))
        {
            return DiscoverFromDeltaLog(blobPrefix, deltaLogReader.Value);
        }

        return DiscoverFromBlobs(containerClient, blobPrefix);
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

    private static bool IsDeltaTable(BlobContainerClient containerClient, string blobPrefix)
    {
        var deltaLogPrefix = blobPrefix + "_delta_log/";
        var deltaLogBlobs = containerClient.GetBlobs(prefix: deltaLogPrefix);
        return deltaLogBlobs.Any();
    }

    private static IEnumerable<Partition> DiscoverFromDeltaLog(string blobPrefix, AzureDeltaLogReader deltaLogReader)
    {
        var snapshot = deltaLogReader.GetLatestSnapshot();
        var partitionGroups = new Dictionary<string, (Dictionary<string, string> Values, List<string> Files)>(StringComparer.OrdinalIgnoreCase);

        foreach (var addAction in snapshot.ActiveFiles)
        {
            var fullFilePath = string.IsNullOrEmpty(blobPrefix) 
                ? addAction.Path 
                : blobPrefix + addAction.Path;
            
            var directory = GetDirectory(fullFilePath);

            if (string.IsNullOrEmpty(directory))
            {
                directory = blobPrefix.TrimEnd('/');
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
            Files = kvp.Value.Files
        });
    }

    private static IEnumerable<Partition> DiscoverFromBlobs(BlobContainerClient containerClient, string blobPrefix)
    {
        var blobs = containerClient.GetBlobs(prefix: blobPrefix);
        var partitionPaths = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        foreach (var blob in blobs)
        {
            if (!HivePartitionParser.IsParquetFile(blob.Name))
            {
                continue;
            }

            var directory = GetDirectory(blob.Name);
            if (!string.IsNullOrEmpty(directory))
            {
                partitionPaths.Add(directory);
            }
        }

        foreach (var path in partitionPaths.OrderBy(p => p))
        {
            var relativePath = path;
            if (!string.IsNullOrEmpty(blobPrefix) && relativePath.StartsWith(blobPrefix, StringComparison.OrdinalIgnoreCase))
            {
                relativePath = relativePath.Substring(blobPrefix.Length);
            }
            
            var values = HivePartitionParser.ParsePartitionValues(relativePath);
            yield return new Partition { Path = path, Values = values };
        }
    }

    private static string GetDirectory(string blobName)
    {
        var lastSlash = blobName.LastIndexOf('/');
        return lastSlash > 0 ? blobName[..lastSlash] : "";
    }
}

