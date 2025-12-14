# ParquetSharpLINQ

[![NuGet](https://img.shields.io/nuget/v/ParquetSharpLINQ.svg)](https://www.nuget.org/packages/ParquetSharpLINQ)
[![NuGet Downloads](https://img.shields.io/nuget/dt/ParquetSharpLINQ.svg)](https://www.nuget.org/packages/ParquetSharpLINQ)
[![Build](https://github.com/naszly/ParquetSharpLINQ/actions/workflows/build.yml/badge.svg)](https://github.com/naszly/ParquetSharpLINQ/actions/workflows/build.yml)
[![Integration Tests](https://github.com/naszly/ParquetSharpLINQ/actions/workflows/integration-tests.yml/badge.svg)](https://github.com/naszly/ParquetSharpLINQ/actions/workflows/integration-tests.yml)
[![License](https://img.shields.io/github/license/naszly/ParquetSharpLINQ.svg)](LICENSE)
![.NET](https://img.shields.io/badge/.NET-8.0%20%7C%209.0%20%7C%2010.0-blue)

A high-performance LINQ provider for querying Hive-partitioned and Delta Lake Parquet files with automatic query optimization.

## Features

- **Source Generated Mappers** - Zero reflection for data mapping
- **Delta Lake Support** - Automatic transaction log reading
- **Partition Pruning** - Only scans matching partitions
- **Column Projection** - Only reads requested columns
- **Type Safe** - Compile-time validation
- **Cross-Platform** - Works on Windows and Linux
- **Azure Blob Storage** - Stream directly from cloud storage

## Quick Start

### Installation

```bash
dotnet add package ParquetSharpLINQ
```

For Azure Blob Storage support:
```bash
dotnet add package ParquetSharpLINQ.Azure
```

### Define Your Entity

```csharp
using ParquetSharpLINQ.Attributes;

public class SalesRecord
{
    [ParquetColumn("id")]
    public long Id { get; set; }
    
    [ParquetColumn("product_name")]
    public string ProductName { get; set; }
    
    [ParquetColumn("total_amount")]
    public decimal TotalAmount { get; set; }
    
    [ParquetColumn("year", IsPartition = true)]
    public int Year { get; set; }
    
    [ParquetColumn("region", IsPartition = true)]
    public string Region { get; set; }
}
```

### Query Local Files

```csharp
using ParquetSharpLINQ;

using var table = ParquetTable<SalesRecord>.Factory.FromFileSystem("/data/sales");

var count = table.Count(s => s.Region == "eu-west" && s.Year == 2024);
var results = table.Where(s => s.TotalAmount > 1000).ToList();
```

### Query Delta Lake Tables

```csharp
using ParquetSharpLINQ;

// Automatically detects and reads _delta_log/
using var table = ParquetTable<SalesRecord>.Factory.FromFileSystem("/data/delta-sales");

var results = table.Where(s => s.Year == 2024).ToList();
```

### Query Azure Blob Storage

```csharp
using ParquetSharpLINQ;
using ParquetSharpLINQ.Azure;  // Extension methods for Azure support

using var table = ParquetTable<SalesRecord>.Factory.FromAzureBlob(
    connectionString: "DefaultEndpointsProtocol=https;AccountName=...",
    containerName: "sales-data");

var results = table.Where(s => s.Year == 2024).ToList();
```

## Performance

Benchmark results with 180 partitions (900K records):

| Query | Partitions Read | Speedup |
|-------|----------------|---------|
| Full scan | 180/180 | 1.0x |
| `region='eu-west'` | 36/180 | ~5x |
| `year=2024 AND region='eu-west'` | 12/180 | ~15x |

Throughput: ~200K records/sec (60K records in ~300ms)

## Directory Structure

Hive-style partitioning:
```
/data/sales/
├── year=2023/
│   ├── region=us-east/
│   │   └── data.parquet
│   └── region=eu-west/
│       └── data.parquet
└── year=2024/
    └── region=us-east/
        └── data.parquet
```

Delta Lake:
```
/data/delta-sales/
├── _delta_log/
│   ├── 00000000000000000000.json
│   └── 00000000000000000001.json
└── year=2024/
    └── data.parquet
```

## Key Features

### Partition Pruning

All LINQ methods with predicates support automatic partition pruning:
- `Count(predicate)`, `LongCount(predicate)`
- `Any(predicate)`, `All(predicate)` 
- `First(predicate)`, `FirstOrDefault(predicate)`
- `Single(predicate)`, `SingleOrDefault(predicate)`
- `Last(predicate)`, `LastOrDefault(predicate)`
- `Where(predicate)`

### Column Projection

Only requested columns are read:

```csharp
var summary = table
    .Select(s => new { s.Id, s.ProductName })
    .ToList();
```

### Automatic Type Conversion

Partition directory names (strings) are converted to property types:
- `"06"` → `6` (int)
- `"2024-12-07"` → `DateTime` or `DateOnly`

### Case-Insensitive Matching

Column names and partition values are case-insensitive. For partition filtering, use lowercase values or case-insensitive comparison:

```csharp
// Recommended: lowercase (matches normalized values)
table.Where(s => s.Region == "us-east")

// Alternative: case-insensitive comparison
table.Where(s => s.Region.Equals("US-EAST", StringComparison.OrdinalIgnoreCase))
```

### Delta Lake Support

Automatically detects `_delta_log/` directory and:
- Reads transaction log files
- Queries only active files (respects deletes/updates)
- Falls back to Hive-style scanning if no Delta log found

**Supported:** Add, Remove, Metadata, Protocol actions  
**Not supported:** Time travel, Checkpoints (uses JSON logs only)

## Testing

```bash
# All tests
dotnet test

# Unit tests only
dotnet test --filter "Category=Unit"

# Integration tests
dotnet test --filter "Category=Integration"
```

See [ParquetSharpLINQ.Tests/README.md](ParquetSharpLINQ.Tests/README.md) for details.

## Benchmarks

```bash
cd ParquetSharpLINQ.Benchmarks

# Generate test data
dotnet run -c Release -- generate ./data 5000

# Run benchmarks  
dotnet run -c Release -- analyze ./data
```

See [ParquetSharpLINQ.Benchmarks/README.md](ParquetSharpLINQ.Benchmarks/README.md) for details.

## Requirements

- .NET 8.0 or higher
- ParquetSharp 21.0.0+

## Architecture

- **ParquetSharpLINQ** - Core LINQ query provider with composition-based design
  - `ParquetTable<T>` - Main queryable interface (implements `IQueryable<T>`)
  - `ParquetTableFactory<T>` - Factory for creating ParquetTable instances
  - `ParquetQueryProvider<T>` - LINQ expression tree visitor and query optimizer
  - `ParquetEnumerationStrategy<T>` - Executes queries with partition pruning and column projection
  - `IPartitionDiscoveryStrategy` - Pluggable partition discovery interface
  - `IParquetReader` - Pluggable Parquet reading interface
  - `FileSystemPartitionDiscovery` - Discovers Hive partitions and Delta logs from local filesystem
  - `ParquetSharpReader` - Reads Parquet files from local filesystem
- **ParquetSharpLINQ.Generator** - Source generator for zero-reflection data mappers
- **ParquetSharpLINQ.Azure** - Azure Blob Storage extension package
  - `ParquetTableFactoryExtensions` - Adds `FromAzureBlob()` factory method
  - `AzureBlobPartitionDiscovery` - Discovers partitions and Delta logs from Azure Blob Storage
  - `AzureBlobParquetReader` - Streams Parquet files from Azure with LRU caching
- **ParquetSharpLINQ.Tests** - Unit and integration tests
- **ParquetSharpLINQ.Benchmarks** - Performance testing

## License

MIT License - see [LICENSE](LICENSE) for details.

## Author

Kornél Naszály - [GitHub](https://github.com/naszly/ParquetSharpLINQ)
