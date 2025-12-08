# ParquetSharpLINQ Benchmarks

Performance testing and benchmarking for ParquetSharpLINQ.

## Quick Start - Local Files

### Generate Test Data

```bash
dotnet run -c Release -- generate ./benchmark_data 5000
```

Generates test data with Hive-style partitioning across multiple years, months, and regions.

### Run Detailed Analysis

```bash
dotnet run -c Release -- analyze ./benchmark_data
```

Runs detailed performance analysis with partition pruning and column projection metrics.

### Cleanup

```bash
dotnet run -c Release -- cleanup ./benchmark_data
```

## Quick Start - Azure Blob Storage

**Azure benchmarks use Azurite by default** - no Azure account needed!

### 1. Start Azurite

```bash
docker run -d -p 10000:10000 --name azurite \
  mcr.microsoft.com/azure-storage/azurite
```

### 2. Upload Test Data to Azurite

```bash
dotnet run -c Release -- azure-upload parquet-bench 5000
```

Generates and uploads test data to Azurite.

### 3. Run Azure Performance Analysis

```bash
dotnet run -c Release -- azure-analyze parquet-bench
```

Tests Azure streaming performance including caching effects.

### 4. Cleanup Azure Data

```bash
dotnet run -c Release -- azure-cleanup parquet-bench
```

### Using Real Azure Storage (Optional)

To use real Azure Storage instead of Azurite, set the `AZURE_STORAGE_CONNECTION_STRING` environment variable:

**Linux/macOS:**
```bash
export AZURE_STORAGE_CONNECTION_STRING="DefaultEndpointsProtocol=https;AccountName=...;AccountKey=..."
dotnet run -c Release -- azure-upload parquet-bench 5000
```

**Windows (PowerShell):**
```powershell
$env:AZURE_STORAGE_CONNECTION_STRING="DefaultEndpointsProtocol=https;AccountName=..."
dotnet run -c Release -- azure-upload parquet-bench 5000
```

## Test Data Structure

```
benchmark_data/
├── year=2023/
│   ├── month=01/
│   │   ├── region=us-east/data.parquet
│   │   ├── region=us-west/data.parquet
│   │   ├── region=eu-central/data.parquet
│   │   ├── region=eu-west/data.parquet
│   │   └── region=ap-southeast/data.parquet
│   ├── month=02/...
│   └── month=12/...
├── year=2024/...
└── year=2025/...
```

**Total:** 180 partitions, 5K-10K records per partition

## Commands Reference

```bash
# Generate with custom size
dotnet run -c Release -- generate ./data 10000

# Detailed analysis
dotnet run -c Release -- analyze ./data

# Cleanup
dotnet run -c Release -- cleanup ./data
```
