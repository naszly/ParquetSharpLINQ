# GitHub Actions CI/CD Pipeline

This repository uses GitHub Actions for continuous integration and automated releases to NuGet.org.

## Workflows

### 1. Build and Test (`build.yml`)

**Triggers:**
- Push to `main` or `develop` branches
- Pull requests to `main`

**What it does:**
1. Sets up .NET 10.0
2. Restores dependencies
3. Builds the solution in Release mode
4. Runs all unit tests
5. Uploads build artifacts

**Status:** ![Build Status](https://github.com/naszly/ParquetSharpLINQ/workflows/Build%20and%20Test/badge.svg)

### 2. Integration Tests (`integration-tests.yml`)

**Triggers:**
- Push to `main` or `develop` branches
- Pull requests to `main`

**What it does:**
1. Sets up .NET 10.0, Python 3.11, and Node.js 20
2. Installs Delta Lake dependencies (pandas, pyarrow, deltalake)
3. Installs and starts Azurite (Azure Storage Emulator)
4. Generates Delta Lake test data using Python
5. Runs all integration tests (including Delta Lake and Azure tests)

**Status:** ![Integration Tests](https://github.com/naszly/ParquetSharpLINQ/workflows/Integration%20Tests/badge.svg)

### 3. Release to NuGet (`release.yml`)

**Triggers:**
- Push of a version tag (e.g., `v1.0.0`, `v1.2.3`)

**What it does:**
1. Builds the solution
2. Runs unit tests
3. Updates package versions based on the tag
4. Creates NuGet packages for:
   - ParquetSharpLINQ
   - ParquetSharpLINQ.Azure
5. Publishes packages to NuGet.org
6. Creates a GitHub Release with package files

**Status:** ![Release](https://github.com/naszly/ParquetSharpLINQ/workflows/Release%20to%20NuGet/badge.svg)

## How to Release a New Version

### Prerequisites

1. **NuGet API Key** - Get from https://www.nuget.org/account/apikeys
2. **Add secret to GitHub** - Go to repository Settings → Secrets → Actions → New secret
   - Name: `NUGET_API_KEY`
   - Value: Your NuGet API key

### Release Process

**Option 1: Using Git Command Line**

```bash
# Make sure your changes are committed and pushed
git add .
git commit -m "Release v1.0.0"
git push

# Create and push a version tag
git tag v1.0.0
git push origin v1.0.0
```

**Option 2: Using GitHub Web Interface**

1. Go to the repository on GitHub
2. Click "Releases" → "Create a new release"
3. Click "Choose a tag" → Type `v1.0.0` → "Create new tag"
4. Fill in release title and description
5. Click "Publish release"

### What Happens Next

1. GitHub Actions detects the tag
2. Release workflow starts automatically
3. Solution is built and tested
4. Package versions are updated to match the tag (e.g., `v1.2.3` → `1.2.3`)
5. NuGet packages are created
6. Packages are pushed to NuGet.org
7. GitHub Release is created with package files attached

### Version Numbering

Use [Semantic Versioning](https://semver.org/):
- `v1.0.0` - Major release (breaking changes)
- `v1.1.0` - Minor release (new features, backward compatible)
- `v1.0.1` - Patch release (bug fixes)

### Pre-release Versions

The workflow **automatically detects** pre-release versions based on the tag format and marks them appropriately.

**Supported pre-release formats:**

```bash
# Alpha releases (early development)
git tag v1.0.0-alpha.1
git push origin v1.0.0-alpha.1

# Beta releases (feature complete, testing)
git tag v1.0.0-beta.1
git push origin v1.0.0-beta.1

# Release candidates (final testing)
git tag v1.0.0-rc.1
git push origin v1.0.0-rc.1

# Custom pre-release identifiers
git tag v2.0.0-preview.1
git push origin v2.0.0-preview.1
```

**What happens with pre-releases:**
- ✅ Published to NuGet.org (users can opt-in with `--prerelease` flag)
- ✅ Marked as "Pre-release" on GitHub (shows orange tag instead of green)
- ✅ Not shown as "Latest" release on GitHub
- ✅ Package version matches tag: `1.0.0-beta.1`

**NuGet pre-release behavior:**

```bash
# Stable releases only (default)
dotnet add package ParquetSharpLINQ

# Include pre-releases
dotnet add package ParquetSharpLINQ --prerelease

# Specific pre-release version
dotnet add package ParquetSharpLINQ --version 1.0.0-beta.1
```

**Detection logic:**
- Any version with a hyphen (`-`) followed by text is considered a pre-release
- Examples: `1.0.0-alpha`, `1.0.0-beta.1`, `2.0.0-rc.2`, `1.5.0-preview`

## Workflow Features

### Automatic Version Updates

The release workflow automatically updates the `<Version>` tag in both `.csproj` files based on the Git tag, so you don't need to manually update version numbers.

### Skip Duplicate Packages

The `--skip-duplicate` flag prevents errors if a package version already exists on NuGet.org.

### Symbol Packages

Both main packages (`.nupkg`) and symbol packages (`.snupkg`) are created and published for debugging support.

### Artifact Upload

Build artifacts are uploaded for 1 day, allowing you to download build outputs for debugging.

### Test Coverage

- **Unit tests** run on every build
- **Integration tests** run on every push/PR (including Delta Lake and Azure tests)
- **Unit tests** run before release to catch issues

## Manual Testing Before Release

Before creating a release tag, you can test locally:

```bash
# Build and test
dotnet build -c Release
dotnet test -c Release --filter "Category=Unit"

# Create packages locally
mkdir -p nupkg
dotnet pack ParquetSharpLINQ/ParquetSharpLINQ.csproj -c Release -o ./nupkg
dotnet pack ParquetSharpLINQ.Azure/ParquetSharpLINQ.Azure.csproj -c Release -o ./nupkg

# List packages
ls -lh nupkg/

# Test package locally (optional)
dotnet nuget push nupkg/ParquetSharpLINQ.1.0.0.nupkg \
  --source ./local-nuget \
  --skip-duplicate
```

## Troubleshooting

### Release workflow fails with "401 Unauthorized"

- Check that `NUGET_API_KEY` secret is set correctly in GitHub
- Verify the API key hasn't expired
- Ensure the API key has push permissions

### Package already exists error

- The workflow uses `--skip-duplicate`, so this shouldn't fail
- If it does, you may need to increment the version number

### Integration tests fail

- Check that Azurite started successfully
- Verify Python dependencies were installed
- Check Delta test data was generated correctly

### Build fails on tag push

- Check that all tests pass locally first
- Verify the version number format is correct (e.g., `v1.0.0`)
- Review workflow logs in GitHub Actions tab

## CI/CD Architecture

```
┌─────────────────┐
│  Git Push/PR    │
└────────┬────────┘
         │
    ┌────┴────┐
    │         │
    ▼         ▼
┌─────────┐ ┌──────────────────┐
│  Build  │ │ Integration Tests│
│  Tests  │ │ • Python setup   │
│         │ │ • Azurite start  │
│         │ │ • Delta data gen │
│         │ │ • Full test suite│
└─────────┘ └──────────────────┘
    
    
┌─────────────────┐
│  Tag Push (vX)  │
└────────┬────────┘
         │
         ▼
┌──────────────────┐
│  Release         │
│  • Build         │
│  • Unit Tests    │
│  • Update Version│
│  • Pack NuGet    │
│  • Push to NuGet │
│  • GitHub Release│
└──────────────────┘
```

## Environment Variables

The workflows use these secrets/variables:

- `NUGET_API_KEY` - NuGet.org API key (required for releases)
- `GITHUB_TOKEN` - Automatically provided by GitHub Actions

## Next Steps

1. ✅ Add `NUGET_API_KEY` to repository secrets
2. ✅ Commit and push the workflow files
3. ✅ Create a test tag to verify the release process
4. ✅ Monitor the GitHub Actions tab for workflow status

## Additional Resources

- [GitHub Actions Documentation](https://docs.github.com/en/actions)
- [Publishing NuGet Packages](https://docs.microsoft.com/en-us/nuget/nuget-org/publish-a-package)
- [Semantic Versioning](https://semver.org/)

