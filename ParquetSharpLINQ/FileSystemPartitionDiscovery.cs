using System.Collections.Immutable;
using ParquetSharpLINQ.Delta;
using ParquetSharpLINQ.Models;

namespace ParquetSharpLINQ;

/// <summary>
/// File system-based partition discovery strategy.
/// Supports both Delta Lake and Hive-style partitioning.
/// Automatically detects Delta tables by checking for _delta_log directory.
/// </summary>
public class FileSystemPartitionDiscovery : IPartitionDiscoveryStrategy
{
    private readonly string _rootPath;
    private readonly Lazy<DeltaLogReader> _deltaLogReader;

    /// <summary>
    /// Creates a new file system partition discovery strategy.
    /// </summary>
    /// <param name="rootPath">Root directory to scan for partitions</param>
    /// <param name="cacheExpiration">Optional cache expiration duration for Delta log (default: 5 minutes)</param>
    public FileSystemPartitionDiscovery(string rootPath, TimeSpan? cacheExpiration = null)
    {
        if (string.IsNullOrEmpty(rootPath))
        {
            throw new ArgumentException("rootPath is required", nameof(rootPath));
        }

        if (!Directory.Exists(rootPath))
        {
            throw new DirectoryNotFoundException(rootPath);
        }

        _rootPath = rootPath;
        _deltaLogReader = new Lazy<DeltaLogReader>(() => new DeltaLogReader(_rootPath, cacheExpiration));
    }

    public IEnumerable<Partition> DiscoverPartitions()
    {
        var deltaLogPath = Path.Combine(_rootPath, "_delta_log");
        if (Directory.Exists(deltaLogPath))
        {
            return DiscoverFromDeltaLog();
        }

        return DiscoverFromFileSystem();
    }

    public void ClearDeltaLogCache()
    {
        if (_deltaLogReader.IsValueCreated)
        {
            _deltaLogReader.Value.ClearCache();
        }
    }

    private IEnumerable<Partition> DiscoverFromDeltaLog()
    {
        var snapshot = _deltaLogReader.Value.GetLatestSnapshot();
        var partitionGroups = new Dictionary<string, (Dictionary<string, string> Values, List<string> Files)>(StringComparer.OrdinalIgnoreCase);

        foreach (var addAction in snapshot.ActiveFiles)
        {
            var fullPath = Path.Combine(_rootPath, addAction.Path);
            
            if (!File.Exists(fullPath))
            {
                continue;
            }

            var directory = Path.GetDirectoryName(fullPath) ?? _rootPath;
            var partitionKey = directory;

            if (!partitionGroups.ContainsKey(partitionKey))
            {
                var partitionValues = addAction.PartitionValues ?? new Dictionary<string, string>();
                partitionGroups[partitionKey] = (
                    new Dictionary<string, string>(partitionValues, StringComparer.OrdinalIgnoreCase),
                    []
                );
            }

            partitionGroups[partitionKey].Files.Add(fullPath);
        }

        return partitionGroups.Select(kvp => new Partition
        {
            Path = Path.GetFullPath(kvp.Key),
            Values = kvp.Value.Values,
            Files = kvp.Value.Files
        });
    }

    private IEnumerable<Partition> DiscoverFromFileSystem()
    {
        var directories = Directory.EnumerateDirectories(_rootPath, "*", SearchOption.AllDirectories)
            .Append(_rootPath)
            .Distinct();

        foreach (var dir in directories)
        {
            var parquetFiles = Directory.EnumerateFiles(dir)
                .Where(HivePartitionParser.IsParquetFile)
                .ToImmutableArray();

            if (!parquetFiles.Any())
            {
                continue;
            }

            var relative = Path.GetRelativePath(_rootPath, dir)
                .TrimEnd(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar);

            var values = HivePartitionParser.ParsePartitionValues(relative);

            yield return new Partition { Path = Path.GetFullPath(dir), Values = values, Files = parquetFiles};
        }
    }
}

