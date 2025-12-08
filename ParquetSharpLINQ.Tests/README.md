# ParquetSharpLINQ Tests

Comprehensive test suite with unit tests and integration tests.

## Directory Structure

```
ParquetSharpLINQ.Tests/
├── Unit/           # Fast unit tests with mocked dependencies
└── Integration/    # Integration tests with real I/O
    ├── IntegrationTests.cs         # Local file tests
    └── AzureIntegrationTests.cs    # Azure/Azurite tests
```

## Running Tests

```bash
# Run all tests
dotnet test

# Run only unit tests (fast)
dotnet test --filter "Category=Unit"

# Run all integration tests
dotnet test --filter "Category=Integration"

# Run only local file integration tests
dotnet test --filter "Category=LocalFiles"

# Run only Azure integration tests (requires Azurite)
dotnet test --filter "Category=Azure"
```

## Azure Integration Tests

Azure integration tests use the Azurite emulator for local testing without requiring an Azure account.

### What is Azurite?

Azurite is an open-source Azure Storage emulator that runs locally. Perfect for:
- Local development and testing
- CI/CD pipelines
- No Azure subscription needed
- No costs
- Fast and reliable

### Quick Start with Azurite

**Using Docker (Recommended):**
```bash
# Start Azurite
docker run -d -p 10000:10000 --name azurite \
  mcr.microsoft.com/azure-storage/azurite

# Run Azure tests
dotnet test --filter "Category=Azure"

# Stop Azurite
docker stop azurite && docker rm azurite
```

**Note:** Azure tests will be automatically skipped if Azurite is not running.

## Troubleshooting

### Azure tests are skipped

Azurite is not running. Start it with:
```bash
docker run -p 10000:10000 mcr.microsoft.com/azure-storage/azurite
```

### Port 10000 already in use

```bash
# Check what's using the port
lsof -i :10000

# Stop existing Azurite
docker stop azurite && docker rm azurite
```

### Connection errors

Verify Azurite is running:
```bash
docker ps | grep azurite
docker logs azurite
```

## Test Data

Integration tests automatically generate and clean up temporary test data:
- **Local tests:** `/tmp/ParquetIntegrationTest_{guid}`
- **Azure tests:** Azurite container `test-{guid}`


