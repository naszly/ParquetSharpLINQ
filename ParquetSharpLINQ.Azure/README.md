# ParquetSharpLINQ.Azure

Azure Blob Storage support for ParquetSharpLINQ.

## Overview

This library extends ParquetSharpLINQ with Azure Blob Storage streaming capabilities, allowing you to query Parquet
files in Azure without downloading them to disk.

## Features

- ✅ Stream Parquet files directly from Azure Blob Storage
- ✅ Zero disk I/O - all data cached in memory
- ✅ Same LINQ API as local files
- ✅ Partition pruning works seamlessly
- ✅ Column projection supported
- ✅ Multiple authentication methods

## Quick Start

```csharp
using ParquetSharpLINQ.Azure;

// Create table from Azure Blob Storage
var connectionString = "DefaultEndpointsProtocol=https;AccountName=...";
using var table = new AzureHiveParquetTable<SalesRecord>(
    connectionString: connectionString,
    containerName: "sales-data"
);

// Query with LINQ - partition pruning works automatically!
var results = table
    .Where(s => s.Year == 2024)  // Only reads year=2024 partitions
    .Where(s => s.Region == "us-east")  // Further filters partitions
    .Select(s => new { s.ProductId, s.Amount })
    .ToList();
```

## Classes

### AzureHiveParquetTable<T>

Convenience wrapper for querying Azure Blob Storage with Hive-style partitioning.

```csharp
// With connection string
var table = new AzureHiveParquetTable<T>(connectionString, containerName);

// With existing BlobContainerClient
var table = new AzureHiveParquetTable<T>(containerClient);
```

### AzureBlobParquetReader

Low-level reader for streaming Parquet files from Azure.

```csharp
var reader = new AzureBlobParquetReader(connectionString, containerName);
var table = new HiveParquetTable<T>("", reader: reader);
```

### AzurePartitionDiscovery

Discovers Hive-style partitions in Azure Blob Storage.

```csharp
var partitions = AzurePartitionDiscovery.Discover(containerClient);
```

## Authentication

### Connection String (Development)

```csharp
var connectionString = "DefaultEndpointsProtocol=https;AccountName=...";
var table = new AzureHiveParquetTable<T>(connectionString, "container");
```

### Managed Identity (Production - Recommended)

```csharp
using Azure.Identity;

var containerClient = new BlobServiceClient(
    new Uri("https://myaccount.blob.core.windows.net"),
    new DefaultAzureCredential()
).GetBlobContainerClient("container");

var table = new AzureHiveParquetTable<T>(containerClient);
```

## Testing with Azurite

Test locally without an Azure subscription using Azurite (Azure Storage Emulator):

```bash
# Start Azurite with Docker
docker run -d -p 10000:10000 --name azurite \
  mcr.microsoft.com/azure-storage/azurite

# Use Azurite connection string
const string AzuriteConnectionString = 
    "DefaultEndpointsProtocol=http;" +
    "AccountName=devstoreaccount1;" +
    "AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;" +
    "BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;";

// Test your code
using var table = new AzureHiveParquetTable<SalesRecord>(
    AzuriteConnectionString,
    "test-container"
);
```

**Benefits:**
- No Azure costs
- Fast local development (~40x faster than real Azure)
- Perfect for CI/CD pipelines
- Works offline

## Dependencies

- ParquetSharpLINQ (core library)
- Azure.Storage.Blobs 12.19.1+

## License

Same as ParquetSharpLINQ core library.

