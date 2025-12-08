# ParquetSharpLINQ

A high-performance LINQ provider for querying Hive-style partitioned Parquet files with automatic query optimization and 100% source generation.

## Features

✅ **100% Source Generated** - Zero reflection overhead  
✅ **Partition Pruning** - Only scans matching partitions (up to 180x faster)  
✅ **Column Projection** - Only reads requested columns  
✅ **Clean LINQ Syntax** - Use `Count(predicate)`, `Any(predicate)`, etc. directly  
✅ **Type Safe** - Compile-time errors for invalid queries  
✅ **DateTime/DateOnly Partitions** - Full support with automatic type conversion  
✅ **Numeric Partitions** - Handles leading zeros (`"06"` matches `6`)  
✅ **Long Path Support** - Handles paths over 260+ characters  
✅ **Case-Insensitive** - Column names work regardless of casing  
✅ **Azure Blob Storage** - Stream from Azure without downloading files  

## Quick Start

### 1. Define Your Entity

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
    
    // Partition columns
    [ParquetColumn("year", IsPartition = true)]
    public int Year { get; set; }
    
    [ParquetColumn("region", IsPartition = true)]
    public string Region { get; set; }
}
```

### 2. Query Your Data

**Local Files:**
```csharp
using var table = new HiveParquetTable<SalesRecord>("/data/sales");

// Clean syntax - partition pruning works automatically!
var count = table.Count(s => s.Region == "eu-west" && s.Year == 2024);
var hasData = table.Any(s => s.Year == 2024);
var record = table.FirstOrDefault(s => s.Id == 12345);

// Traditional syntax also works
var euSales = table
    .Where(s => s.Region == "eu-west" && s.Year == 2024)
    .Where(s => s.TotalAmount > 1000)
    .ToList();
```

**Azure Blob Storage:**
```csharp
using ParquetSharpLINQ.Azure;

// Stream directly from Azure - no disk downloads!
using var table = new AzureHiveParquetTable<SalesRecord>(
    connectionString: "DefaultEndpointsProtocol=https;AccountName=...",
    containerName: "sales-data"
);

// Same LINQ syntax works!
var results = table
    .Where(s => s.Year == 2024)
    .Where(s => s.Region == "us-east")
    .ToList();
```

## Performance

### Partition Pruning Results

With 180 partitions (900K records):

| Query | Partitions Scanned | Data Reduction | Speedup |
|-------|-------------------|----------------|---------|
| Full scan | 180/180 (100%) | 0% | 1x |
| `region='eu-west'` | 36/180 (20%) | 80% | ~5x |
| `year=2024 AND region='eu-west'` | 12/180 (6.7%) | 93% | ~15x |

**Real benchmark:** 60,000 records read in ~300ms (~190K records/sec)

### Supported LINQ Methods with Partition Pruning

All these methods support partition pruning when using predicates:

- `Count(predicate)` / `LongCount(predicate)`
- `Any(predicate)` / `All(predicate)`
- `First(predicate)` / `FirstOrDefault(predicate)`
- `Single(predicate)` / `SingleOrDefault(predicate)`
- `Last(predicate)` / `LastOrDefault(predicate)`
- `Where(predicate)` (traditional)

### Column Projection

Only reads the columns you need:

```csharp
// Reads only 3 of 8 columns - 62% I/O reduction
var summary = table
    .Select(s => new { s.Id, s.ProductName, s.TotalAmount })
    .ToList();
```

## Installation

**Core Library:**
```bash
dotnet add package ParquetSharp
```

**Azure Blob Storage Support (optional):**
```bash
dotnet add package Azure.Storage.Blobs
# Add reference to ParquetSharpLINQ.Azure project
```

Add project references to `ParquetSharpLINQ` and `ParquetSharpLINQ.Generator`.

## Directory Structure

```
/data/sales/
├── year=2023/
│   ├── region=us-east/
│   │   └── data.parquet
│   └── region=eu-west/
│       └── data.parquet
├── year=2024/
│   ├── region=us-east/
│   │   └── data.parquet
│   └── region=eu-west/
│       └── data.parquet
```

## Key Features Explained

### Automatic Type Conversion

Partition values are strings in directory names, but automatically converted to property types:

- `"06"` → `6` (int)
- `"2024-12-07"` → `DateTime(2024, 12, 7)`
- `"2024-12-07"` → `DateOnly(2024, 12, 7)`

### Case-Insensitive Column Matching

All column names are case-insensitive:

```csharp
// These all match the same column
"clientId" == "ClientId" == "CLIENTID" == "client_id"
```

### Cross-Platform Partition Handling

**Partition values are normalized to lowercase for consistency across platforms:**

```csharp
// Directories use lowercase (standard convention): region=us-east

// ✅ CORRECT: Use lowercase in queries to match normalized values
table.Where(s => s.Region == "us-east")  // Works - returns records

// ❌ INCORRECT: Uppercase won't match normalized lowercase values
table.Where(s => s.Region == "US-EAST")  // Returns empty - case mismatch

// ✅ ALTERNATIVE: Use case-insensitive comparison if you need flexibility
table.Where(s => s.Region.Equals("US-EAST", StringComparison.OrdinalIgnoreCase))  // Works!
```

**How it works:**
- **Partition values are normalized to lowercase** when loaded into records
- **Ensures consistent behavior** across Windows and Linux
- **Matches standard convention** of using lowercase directory names

**Cross-Platform Compatibility:**
- **Windows:** File system is case-insensitive (`region=us-east` and `region=US-EAST` are the same)
- **Linux:** File system is case-sensitive (they would be different directories)
- **ParquetSharpLINQ:** Normalizes partition values to lowercase, ensuring queries work the same on both platforms

**Best Practices:**

1. **Use lowercase everywhere** (recommended):
```csharp
// Directory: year=2024/region=us-east
table.Where(s => s.Region == "us-east")  // ✅ Best practice
```

2. **Use case-insensitive comparison** (when you need flexibility):
```csharp
// Allows any casing in your queries
table.Where(s => s.Region.Equals("US-EAST", StringComparison.OrdinalIgnoreCase))  // ✅ Works
table.Where(s => s.Region.Equals("Us-EaSt", StringComparison.OrdinalIgnoreCase))  // ✅ Works
```

3. **Avoid mixed casing in directory names**:
```
✅ Recommended: year=2024/region=us-east
❌ Avoid: Year=2024/Region=US-EAST
```


### Safe Partition Handling

The library uses a special prefix (`\0`) for partition values that's impossible in valid Parquet column names, preventing any collision with actual data.

## Testing

```bash
# Run all tests
dotnet test

# Run unit tests only (fast)
dotnet test --filter "Category=Unit"

# Run integration tests
dotnet test --filter "Category=Integration"

# Run Azure integration tests (requires Azurite)
dotnet test --filter "Category=Azure"
```


See [ParquetSharpLINQ.Tests/README.md](ParquetSharpLINQ.Tests/README.md) for complete testing guide.

## Benchmarks

```bash
cd ParquetSharpLINQ.Benchmarks

# Generate test data (900K records across 180 partitions)
dotnet run -c Release -- generate ./benchmark_data 5000

# Run performance analysis
dotnet run -c Release -- analyze ./benchmark_data

# Cleanup
dotnet run -c Release -- cleanup ./benchmark_data
```

See [ParquetSharpLINQ.Benchmarks/README.md](ParquetSharpLINQ.Benchmarks/README.md) for detailed performance testing guide.

## Documentation

- **[ParquetSharpLINQ.Azure/README.md](ParquetSharpLINQ.Azure/README.md)** - Azure Blob Storage support
- **[ParquetSharpLINQ.Benchmarks/README.md](ParquetSharpLINQ.Benchmarks/README.md)** - Performance benchmarks
- **[ParquetSharpLINQ.Tests/README.md](ParquetSharpLINQ.Tests/README.md)** - Test suite guide

## Requirements

- .NET 10.0
- ParquetSharp 21.0.0

## Architecture

- **ParquetSharpLINQ** - Core library with LINQ query provider
- **ParquetSharpLINQ.Azure** - Azure Blob Storage support (optional)
- **ParquetSharpLINQ.Benchmarks** - Performance testing and benchmarks
- **ParquetSharpLINQ.Generator** - Source generator for zero-reflection mappers
- **ParquetSharpLINQ.Tests** - Unit and integration tests
