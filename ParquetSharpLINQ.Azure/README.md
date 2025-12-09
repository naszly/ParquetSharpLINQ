# ParquetSharpLINQ.Azure

[![NuGet](https://img.shields.io/nuget/v/ParquetSharpLINQ.Azure.svg)](https://www.nuget.org/packages/ParquetSharpLINQ.Azure)
[![NuGet Downloads](https://img.shields.io/nuget/dt/ParquetSharpLINQ.Azure.svg)](https://www.nuget.org/packages/ParquetSharpLINQ.Azure)
![.NET](https://img.shields.io/badge/.NET-8.0%20%7C%209.0%20%7C%2010.0-blue)

Azure Blob Storage support for ParquetSharpLINQ with Delta Lake.

## Overview

This library extends ParquetSharpLINQ with Azure Blob Storage streaming capabilities, allowing you to query Parquet and Delta Lake tables in Azure without downloading them to disk.

## Features

- ✅ Stream Parquet files directly from Azure Blob Storage
- ✅ **Delta Lake Support** - Automatic Delta transaction log reading from Azure
- ✅ Zero disk I/O - all data cached in memory
- ✅ Same LINQ API as local files
- ✅ Partition pruning works seamlessly
- ✅ Column projection supported
- ✅ Multiple authentication methods

## Quick Start

**Hive-style Parquet Files:**
```csharp
using ParquetSharpLINQ.Azure;

// Create table from Azure Blob Storage
var connectionString = "DefaultEndpointsProtocol=https;AccountName=...";
using var table = new AzureBlobParquetTable<SalesRecord>(
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

**Delta Lake Tables:**
```csharp
using ParquetSharpLINQ.Azure;

// Delta Lake tables work automatically - just point to the container
using var deltaTable = new AzureBlobParquetTable<SalesRecord>(
    connectionString: connectionString,
    containerName: "delta-sales"  // Container with _delta_log/ prefix
);

// Delta transaction log is read automatically from Azure
// Only active files (after updates/deletes) are queried
var results = deltaTable
    .Where(s => s.Year == 2024)
    .ToList();
```

## Classes

### AzureBlobParquetTable<T>

Parquet table for querying Azure Blob Storage with Hive-style partitioning and Delta Lake support.

```csharp
// With connection string
var table = new AzureBlobParquetTable<T>(connectionString, containerName);

// With existing BlobContainerClient
var table = new AzureBlobParquetTable<T>(containerClient);

// With blob prefix (subfolder)
var table = new AzureBlobParquetTable<T>(connectionString, containerName, "data/sales/");
```

**Delta Lake Support:**
- Automatically detects `_delta_log/` blobs in the container
- Reads Delta transaction logs from Azure Blob Storage
- Only queries active files according to the Delta log

### AzureBlobParquetReader

Low-level reader for streaming Parquet files from Azure.

```csharp
var reader = new AzureBlobParquetReader(connectionString, containerName);
var table = new ParquetTable<T>("", reader: reader);
```

### AzurePartitionDiscovery

Discovers Hive-style partitions and Delta Lake tables in Azure Blob Storage.

```csharp
var partitions = AzurePartitionDiscovery.Discover(containerClient);
```

**Supports:**
- Hive-style partitioning (`year=2024/region=us-east/`)
- Delta Lake transaction logs (`_delta_log/*.json`)

## Authentication

### Connection String (Development)

```csharp
var connectionString = "DefaultEndpointsProtocol=https;AccountName=...";
var table = new AzureBlobParquetTable<T>(connectionString, "container");
```

### Managed Identity (Production - Recommended)

```csharp
using Azure.Identity;

var containerClient = new BlobServiceClient(
    new Uri("https://myaccount.blob.core.windows.net"),
    new DefaultAzureCredential()
).GetBlobContainerClient("container");

var table = new AzureBlobParquetTable<T>(containerClient);
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
using var table = new AzureBlobParquetTable<SalesRecord>(
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

- ParquetSharpLINQ (core library with Delta Lake support)
- Azure.Storage.Blobs 12.19.1+

## License

Same as ParquetSharpLINQ core library.

