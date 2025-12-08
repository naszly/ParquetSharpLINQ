using Azure.Storage.Blobs;
using ParquetSharpLINQ.Delta;

namespace ParquetSharpLINQ.Azure;

/// <summary>
/// Reads Delta Lake transaction logs from Azure Blob Storage.
/// </summary>
public class AzureDeltaLogReader
{
    private readonly BlobContainerClient _containerClient;
    private const string DeltaLogPrefix = "_delta_log/";

    public AzureDeltaLogReader(BlobContainerClient containerClient)
    {
        _containerClient = containerClient ?? throw new ArgumentNullException(nameof(containerClient));
    }

    public DeltaSnapshot GetLatestSnapshot()
    {
        var allActions = GetLogFiles()
            .OrderBy(f => f.Version)
            .SelectMany(f => ReadLogFile(f.BlobName));
        
        return DeltaLogParser.BuildSnapshot(allActions);
    }

    private IEnumerable<(string BlobName, long Version)> GetLogFiles()
    {
        var blobs = _containerClient.GetBlobs(prefix: DeltaLogPrefix);

        foreach (var blob in blobs)
        {
            if (blob.Name.EndsWith(".json", StringComparison.OrdinalIgnoreCase))
            {
                var version = DeltaLogParser.ExtractVersion(blob.Name);
                if (version >= 0)
                {
                    yield return (blob.Name, version);
                }
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

