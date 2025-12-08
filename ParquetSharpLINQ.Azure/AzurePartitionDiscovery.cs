using Azure.Storage.Blobs;
using ParquetSharpLINQ.Common;
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
    /// <returns>Enumerable of discovered partitions</returns>
    public static IEnumerable<Partition> Discover(BlobContainerClient containerClient)
    {
        if (containerClient == null)
        {
            throw new ArgumentNullException(nameof(containerClient));
        }

        if (IsDeltaTable(containerClient))
        {
            return DiscoverFromDeltaLog(containerClient);
        }

        return DiscoverFromBlobs(containerClient);
    }

    private static bool IsDeltaTable(BlobContainerClient containerClient)
    {
        var deltaLogBlobs = containerClient.GetBlobs(prefix: "_delta_log/");
        return deltaLogBlobs.Any();
    }

    private static IEnumerable<Partition> DiscoverFromDeltaLog(BlobContainerClient containerClient)
    {
        var deltaReader = new AzureDeltaLogReader(containerClient);
        var snapshot = deltaReader.GetLatestSnapshot();
        var partitionGroups = new Dictionary<string, (Dictionary<string, string> Values, List<string> Files)>(StringComparer.OrdinalIgnoreCase);

        foreach (var addAction in snapshot.ActiveFiles)
        {
            var directory = GetDirectory(addAction.Path);
            
            if (string.IsNullOrEmpty(directory))
            {
                directory = "";
            }

            if (!partitionGroups.ContainsKey(directory))
            {
                var partitionValues = addAction.PartitionValues ?? new Dictionary<string, string>();
                partitionGroups[directory] = (
                    new Dictionary<string, string>(partitionValues, StringComparer.OrdinalIgnoreCase),
                    []
                );
            }

            partitionGroups[directory].Files.Add(addAction.Path);
        }

        return partitionGroups.Select(kvp => new Partition
        {
            Path = kvp.Key,
            Values = kvp.Value.Values,
            Files = kvp.Value.Files
        });
    }

    private static IEnumerable<Partition> DiscoverFromBlobs(BlobContainerClient containerClient)
    {
        var blobs = containerClient.GetBlobs();
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
            var values = HivePartitionParser.ParsePartitionValues(path);
            yield return new Partition { Path = path, Values = values };
        }
    }

    private static string GetDirectory(string blobName)
    {
        var lastSlash = blobName.LastIndexOf('/');
        return lastSlash > 0 ? blobName[..lastSlash] : "";
    }
}

