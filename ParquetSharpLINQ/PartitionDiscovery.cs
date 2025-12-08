using ParquetSharpLINQ.Common;
using ParquetSharpLINQ.Models;

namespace ParquetSharpLINQ;

/// <summary>
/// Simple hive-style partition discovery.
/// Scans directories recursively and treats directories that contain parquet files as partition leaves.
/// For each leaf directory, parses path segments of the form key=value and collects partition key-values.
/// </summary>
public static class PartitionDiscovery
{
    /// <summary>
    /// Discover leaf partitions under the root path.
    /// A leaf is any directory that contains files with extension `.parquet` (case-insensitive).
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

        var directories = Directory.EnumerateDirectories(rootPath, "*", SearchOption.AllDirectories)
            .Append(rootPath)
            .Distinct();

        foreach (var dir in directories)
        {
            var parquetFiles = Directory.EnumerateFiles(dir)
                .Where(HivePartitionParser.IsParquetFile);

            if (!parquetFiles.Any())
            {
                continue;
            }

            var relative = Path.GetRelativePath(rootPath, dir)
                .TrimEnd(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar);

            var values = HivePartitionParser.ParsePartitionValues(relative);

            yield return new Partition { Path = Path.GetFullPath(dir), Values = values };
        }
    }
}

