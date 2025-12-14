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
using ParquetSharpLINQ;
using ParquetSharpLINQ.Azure;

// Create table from Azure Blob Storage
var connectionString = "DefaultEndpointsProtocol=https;AccountName=...";
using var table = ParquetTable<SalesRecord>.Factory.FromAzureBlob(
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
using ParquetSharpLINQ;
using ParquetSharpLINQ.Azure;

// Delta Lake tables work automatically - just point to the container
using var deltaTable = ParquetTable<SalesRecord>.Factory.FromAzureBlob(
    connectionString: connectionString,
    containerName: "delta-sales"  // Container with _delta_log/ prefix
);

// Delta transaction log is read automatically from Azure
// Only active files (after updates/deletes) are queried
var results = deltaTable
    .Where(s => s.Year == 2024)
    .ToList();
```

## Factory Methods

### ParquetTable\<T\>.Factory.FromAzureBlob()

Creates a ParquetTable for querying Parquet files from Azure Blob Storage with Hive-style partitioning and Delta Lake support.

**With connection string:**
```csharp
var table = ParquetTable<SalesRecord>.Factory.FromAzureBlob(
    connectionString: "DefaultEndpointsProtocol=https;AccountName=...",
    containerName: "sales-data"
);
```

**With existing BlobContainerClient:**
```csharp
var containerClient = new BlobContainerClient(connectionString, containerName);
var table = ParquetTable<SalesRecord>.Factory.FromAzureBlob(containerClient);
```

**With blob prefix (subfolder):**
```csharp
var table = ParquetTable<SalesRecord>.Factory.FromAzureBlob(
    connectionString: connectionString,
    containerName: "data",
    blobPrefix: "sales/2024/"
);
```

**With custom cache settings:**
```csharp
var table = ParquetTable<SalesRecord>.Factory.FromAzureBlob(
    connectionString: connectionString,
    containerName: "sales-data",
    blobPrefix: "",
    cacheExpiration: TimeSpan.FromMinutes(10),
    maxCacheSizeBytes: 8L * 1024 * 1024 * 1024  // 8 GB
);
```

**Features:**
- Automatically configures optimized BlobContainerClient with HTTP/2, connection pooling, and retry logic
- Supports both Hive-style partitioning and Delta Lake
- Automatically detects `_delta_log/` blobs in the container
- Reads Delta transaction logs from Azure Blob Storage
- Only queries active files according to the Delta log

## Core Components

### AzureBlobParquetReader

Low-level reader for streaming Parquet files from Azure with file-based caching and LRU eviction.

```csharp
var containerClient = new BlobContainerClient(connectionString, containerName);
var reader = new AzureBlobParquetReader(containerClient, maxCacheSizeBytes: 4L * 1024 * 1024 * 1024);
var discoveryStrategy = new AzureBlobPartitionDiscovery(containerClient);
var table = new ParquetTable<SalesRecord>(discoveryStrategy, reader);
```

### AzureBlobPartitionDiscovery

Discovers Hive-style partitions and Delta Lake tables in Azure Blob Storage.

**Features:**
- Hive-style partitioning (`year=2024/region=us-east/`)
- Delta Lake transaction logs (`_delta_log/*.json`)

## Authentication

### Connection String (Development)

```csharp
var connectionString = "DefaultEndpointsProtocol=https;AccountName=...";
var table = ParquetTable<SalesRecord>.Factory.FromAzureBlob(connectionString, "container");
```

### Managed Identity (Production - Recommended)

```csharp
using Azure.Identity;

var containerClient = new BlobServiceClient(
    new Uri("https://myaccount.blob.core.windows.net"),
    new DefaultAzureCredential()
).GetBlobContainerClient("container");

var table = ParquetTable<SalesRecord>.Factory.FromAzureBlob(containerClient);
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
using var table = ParquetTable<SalesRecord>.Factory.FromAzureBlob(
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

