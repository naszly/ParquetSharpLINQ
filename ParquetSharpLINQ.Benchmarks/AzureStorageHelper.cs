using Azure.Storage.Blobs;
using ParquetSharpLINQ.DataGenerator;

namespace ParquetSharpLINQ.Benchmarks;

/// <summary>
/// Helper class to upload test data to Azure Blob Storage for benchmarking
/// </summary>
public static class AzureStorageHelper
{
    private static void UploadTestData(string connectionString, string containerName, string localPath,
        string prefix = "")
    {
        Console.WriteLine("Uploading test data to Azure Blob Storage...");
        Console.WriteLine($"  Container: {containerName}");
        Console.WriteLine($"  Prefix: {prefix ?? "(root)"}");

        var containerClient = new BlobServiceClient(connectionString).GetBlobContainerClient(containerName);

        // Create container if it doesn't exist
        containerClient.CreateIfNotExists();

        // Upload all parquet files
        var files = Directory.GetFiles(localPath, "*.parquet", SearchOption.AllDirectories);
        var uploaded = 0;

        foreach (var file in files)
        {
            var relativePath = Path.GetRelativePath(localPath, file);
            var blobPath = string.IsNullOrEmpty(prefix)
                ? relativePath.Replace('\\', '/')
                : $"{prefix}/{relativePath.Replace('\\', '/')}";

            var blobClient = containerClient.GetBlobClient(blobPath);

            using var fileStream = File.OpenRead(file);
            blobClient.Upload(fileStream, true);

            uploaded++;
            if (uploaded % 10 == 0) Console.Write($"\r  Uploaded: {uploaded}/{files.Length} files");
        }

        Console.WriteLine($"\r  Uploaded: {uploaded}/{files.Length} files");
        Console.WriteLine("Upload complete!");
    }

    public static void CleanupAzureData(string connectionString, string containerName, string prefix = "")
    {
        Console.WriteLine("Cleaning up Azure Blob Storage...");
        Console.WriteLine($"  Container: {containerName}");
        Console.WriteLine($"  Prefix: {prefix ?? "(root)"}");

        var containerClient = new BlobServiceClient(connectionString).GetBlobContainerClient(containerName);

        if (!containerClient.Exists())
        {
            Console.WriteLine("  Container does not exist, nothing to clean up");
            return;
        }

        var blobs = containerClient.GetBlobs(prefix: prefix);
        var deleted = 0;

        foreach (var blob in blobs)
        {
            containerClient.DeleteBlob(blob.Name);
            deleted++;

            if (deleted % 10 == 0) Console.Write($"\r  Deleted: {deleted} blobs");
        }

        Console.WriteLine($"\r  Deleted: {deleted} blobs");
        Console.WriteLine("Cleanup complete!");
    }

    public static void GenerateAndUpload(
        string connectionString,
        string containerName,
        int recordsPerPartition = 5000,
        int[] years = null!,
        string prefix = "benchmark_data")
    {
        // Generate data locally first
        var tempPath = Path.Combine(Path.GetTempPath(), $"parquet_temp_{Guid.NewGuid():N}");

        try
        {
            Console.WriteLine("Generating test data locally...");
            var generator = new TestDataGenerator();
            years ??= Enumerable.Range(2023, 3).ToArray();
            generator.GenerateParquetFiles(tempPath, recordsPerPartition, years);

            // Upload to Azure
            UploadTestData(connectionString, containerName, tempPath, prefix);

            Console.WriteLine("\nTest data ready in Azure:");
            Console.WriteLine($"  Container: {containerName}");
            Console.WriteLine($"  Prefix: {prefix}");
            Console.WriteLine($"  Years: {string.Join(", ", years)}");
            Console.WriteLine($"  Records per partition: {recordsPerPartition:N0}");

            // Calculate total partitions and records
            var totalPartitions = years.Length * 12 * 5; // years * months * regions
            var totalRecords = totalPartitions * recordsPerPartition;
            Console.WriteLine($"  Total partitions: {totalPartitions}");
            Console.WriteLine($"  Total records: {totalRecords:N0}");
        }
        finally
        {
            // Cleanup local temp data
            if (Directory.Exists(tempPath)) Directory.Delete(tempPath, true);
        }
    }
}