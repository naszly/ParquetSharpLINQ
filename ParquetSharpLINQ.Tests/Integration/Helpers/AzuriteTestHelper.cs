using Azure.Storage.Blobs;

namespace ParquetSharpLINQ.Tests.Integration.Helpers;

/// <summary>
/// Common helper for Azurite emulator integration tests.
/// </summary>
public static class AzuriteTestHelper
{
    public const string ConnectionString =
        "DefaultEndpointsProtocol=http;" +
        "AccountName=devstoreaccount1;" +
        "AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;" +
        "BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;";

    public static async Task<BlobContainerClient> CreateContainerAsync(string prefix = "test")
    {
        if (!await IsRunningAsync())
        {
            throw new InvalidOperationException(
                "Azurite is not running. Start it with: docker run -p 10000:10000 mcr.microsoft.com/azure-storage/azurite");
        }

        var containerName = GenerateContainerName(prefix);
        var serviceClient = new BlobServiceClient(ConnectionString);
        var containerClient = serviceClient.GetBlobContainerClient(containerName);
        await containerClient.CreateAsync();

        return containerClient;
    }
    
    private static async Task<bool> IsRunningAsync()
    {
        try
        {
            var client = new BlobServiceClient(ConnectionString);
            await client.GetPropertiesAsync();
            return true;
        }
        catch
        {
            return false;
        }
    }
    
    private static string GenerateContainerName(string prefix = "test")
    {
        return $"{prefix}-{Guid.NewGuid():N}";
    }

    public static async Task DeleteContainerAsync(BlobContainerClient? containerClient)
    {
        if (containerClient != null && await containerClient.ExistsAsync())
        {
            await containerClient.DeleteAsync();
        }
    }
}

