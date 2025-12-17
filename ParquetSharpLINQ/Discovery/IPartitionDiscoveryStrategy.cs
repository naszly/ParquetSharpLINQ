using ParquetSharpLINQ.Models;

namespace ParquetSharpLINQ.Discovery;

/// <summary>
/// Strategy interface for discovering partitions from different storage backends.
/// Implementations include file system and Azure Blob Storage discovery.
/// </summary>
public interface IPartitionDiscoveryStrategy
{
    /// <summary>
    /// Discovers all partitions in the configured storage location.
    /// Supports both Hive-style partitioning and Delta Lake tables.
    /// </summary>
    /// <returns>Enumerable of discovered partitions with their files and metadata.</returns>
    IEnumerable<Partition> DiscoverPartitions();
}

