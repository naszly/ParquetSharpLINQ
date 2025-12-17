using Azure.Storage.Blobs;
using ParquetSharpLINQ.Delta;

namespace ParquetSharpLINQ.Azure;

/// <summary>
/// Reads Delta Lake transaction logs from Azure Blob Storage.
/// </summary>
public class AzureDeltaLogReader
{
    private readonly BlobContainerClient _containerClient;
    private readonly string _deltaLogPrefix;

    public AzureDeltaLogReader(BlobContainerClient containerClient, string blobPrefix = "")
    {
        _containerClient = containerClient ?? throw new ArgumentNullException(nameof(containerClient));
        
        blobPrefix = blobPrefix?.Trim() ?? "";
        if (!string.IsNullOrEmpty(blobPrefix))
        {
            if (blobPrefix.StartsWith('/'))
            {
                blobPrefix = blobPrefix.TrimStart('/');
            }
            if (!blobPrefix.EndsWith('/'))
            {
                blobPrefix += '/';
            }
        }
        
        _deltaLogPrefix = blobPrefix + "_delta_log/";
    }

    /// <summary>
    /// Gets the latest Delta Lake snapshot by reading and parsing all transaction log files.
    /// Uses cached data when available to minimize Azure Storage API calls.
    /// </summary>
    /// <returns>A Delta snapshot containing active files, metadata, and protocol information.</returns>
    public DeltaSnapshot GetLatestSnapshot()
    {
        var allActions = GetLogFiles()
            .SelectMany(f => ReadLogFile(f.BlobName));
        
        return DeltaLogParser.BuildSnapshot(allActions);
    }

    private IEnumerable<(string BlobName, long Version)> GetLogFiles()
    {
        var blobs = _containerClient.GetBlobs(prefix: _deltaLogPrefix);

        foreach (var blob in blobs)
        {
            if (!blob.Name.EndsWith(".json", StringComparison.OrdinalIgnoreCase))
            {
                continue;
            }
            
            var version = DeltaLogParser.ExtractVersion(blob.Name);
            if (version >= 0)
            {
                yield return (blob.Name, version);
            }
        }
    }

    private IEnumerable<DeltaAction> ReadLogFile(string blobName)
    {
        var blobClient = _containerClient.GetBlobClient(blobName);
        
        if (!blobClient.Exists())
        {
            yield break;
        }

        using var stream = new MemoryStream();
        blobClient.DownloadTo(stream);
        stream.Position = 0;

        using var reader = new StreamReader(stream);
        
        foreach (var action in DeltaLogParser.ParseActionsFromStream(reader))
        {
            yield return action;
        }
    }
}

