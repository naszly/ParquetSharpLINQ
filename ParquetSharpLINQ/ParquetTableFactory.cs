using ParquetSharpLINQ.Discovery;
using ParquetSharpLINQ.Interfaces;
using ParquetSharpLINQ.ParquetSharp;

namespace ParquetSharpLINQ;

/// <summary>
/// Factory for creating ParquetTable instances.
/// Access via ParquetTable&lt;T&gt;.Factory property.
/// Can be extended via extension methods by storage-specific implementations (e.g., Azure).
/// </summary>
public class ParquetTableFactory<T> where T : new()
{
    /// <summary>
    /// Creates a new ParquetTable for querying Parquet files from the file system.
    /// Supports both Hive-style partitioning and Delta Lake tables.
    /// </summary>
    /// <param name="rootPath">Root directory containing Parquet files</param>
    /// <param name="cacheExpiration">Optional cache expiration for Delta log (default: 5 minutes)</param>
    /// <param name="mapper">Optional custom mapper (for DI/testing). If null, uses source-generated mapper.</param>
    /// <returns>A new ParquetTable instance for querying local files</returns>
    /// <example>
    /// var table = ParquetTable&lt;SalesRecord&gt;.Factory.FromFileSystem("/data/sales");
    /// </example>
    public ParquetTable<T> FromFileSystem(
        string rootPath,
        TimeSpan? cacheExpiration = null,
        IParquetMapper<T>? mapper = null)
    {
        var discoveryStrategy = new FileSystemPartitionDiscovery(rootPath, cacheExpiration);
        var reader = new ParquetSharpReader();
        return new ParquetTable<T>(discoveryStrategy, reader, mapper);
    }
}

