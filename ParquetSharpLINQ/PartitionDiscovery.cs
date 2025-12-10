using System.Collections.Immutable;
using ParquetSharpLINQ.Delta;
using ParquetSharpLINQ.Models;

namespace ParquetSharpLINQ;

/// <summary>
/// Partition discovery that supports both Delta Lake and Hive-style partitioning.
/// Automatically detects Delta tables by checking for _delta_log directory.
/// </summary>
public static class PartitionDiscovery
{
    /// <summary>
    /// Discover partitions under the root path.
    /// If a _delta_log directory exists, reads from Delta transaction log.
    /// Otherwise, scans directories for Parquet files (Hive-style).
    /// </summary>
    /// <param name="rootPath">Root directory to scan.</param>
    /// <returns>Enumerable of discovered Partition objects.</returns>
    public static IEnumerable<Partition> Discover(string rootPath)
    {
        if (string.IsNullOrEmpty(rootPath))
        {
            throw new ArgumentException("rootPath is required", nameof(rootPath));
        }

        if (!Directory.Exists(rootPath))
        {
            throw new DirectoryNotFoundException(rootPath);
        }

        var deltaLogPath = Path.Combine(rootPath, "_delta_log");
        if (Directory.Exists(deltaLogPath))
        {
            return DiscoverFromDeltaLog(rootPath);
        }

        return DiscoverFromFileSystem(rootPath);
    }

    private static IEnumerable<Partition> DiscoverFromDeltaLog(string rootPath)
    {
        var deltaReader = new DeltaLogReader(rootPath);
        var snapshot = deltaReader.GetLatestSnapshot();
        var partitionGroups = new Dictionary<string, (Dictionary<string, string> Values, List<string> Files)>(StringComparer.OrdinalIgnoreCase);

        foreach (var addAction in snapshot.ActiveFiles)
        {
            var fullPath = Path.Combine(rootPath, addAction.Path);
            
            if (!File.Exists(fullPath))
            {
                continue;
            }

            var directory = Path.GetDirectoryName(fullPath) ?? rootPath;
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

    private static IEnumerable<Partition> DiscoverFromFileSystem(string rootPath)
    {
        var directories = Directory.EnumerateDirectories(rootPath, "*", SearchOption.AllDirectories)
            .Append(rootPath)
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

            var relative = Path.GetRelativePath(rootPath, dir)
                .TrimEnd(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar);

            var values = HivePartitionParser.ParsePartitionValues(relative);

            yield return new Partition { Path = Path.GetFullPath(dir), Values = values, Files = parquetFiles};
        }
    }
}

