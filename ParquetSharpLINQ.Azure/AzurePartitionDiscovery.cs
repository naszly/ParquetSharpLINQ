using Azure.Storage.Blobs;
using ParquetSharpLINQ.Common;
using ParquetSharpLINQ.Models;

namespace ParquetSharpLINQ.Azure;

/// <summary>
/// Discovers Hive-style partitions from Azure Blob Storage.
/// </summary>
public static class AzurePartitionDiscovery
{
    /// <summary>
    /// Discovers partitions from Azure Blob Storage container.
    /// </summary>
    /// <param name="containerClient">Azure Blob Container client</param>
    /// <returns>Enumerable of discovered partitions</returns>
    public static IEnumerable<Partition> Discover(BlobContainerClient containerClient)
    {
        if (containerClient == null)
        {
            throw new ArgumentNullException(nameof(containerClient));
        }

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

