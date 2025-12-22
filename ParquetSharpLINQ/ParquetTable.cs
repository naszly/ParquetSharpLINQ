using ParquetSharpLINQ.Discovery;
using ParquetSharpLINQ.Interfaces;
using ParquetSharpLINQ.Mappers;
using System.Collections;
using System.Linq.Expressions;
using ParquetSharpLINQ.Models;
using ParquetSharpLINQ.Query;

namespace ParquetSharpLINQ;

public sealed class ParquetTable<T> : IOrderedQueryable<T>, IDisposable where T : new()
{
    private readonly ParquetEnumerationStrategy<T> _enumerationStrategy;

    /// <summary>
    /// Gets the factory for creating ParquetTable instances.
    /// Use ParquetTable&lt;T&gt;.Factory.FromFileSystem() or ParquetTable&lt;T&gt;.Factory.FromAzureBlob() (with Azure package).
    /// </summary>
    public static ParquetTableFactory<T> Factory { get; } = new();
    
    /// <summary>
    /// Creates a new ParquetTable with custom strategy and reader implementations.
    /// Advanced constructor for dependency injection and testing scenarios.
    /// </summary>
    /// <param name="discoveryStrategy">Partition discovery strategy</param>
    /// <param name="reader">Parquet reader implementation</param>
    /// <param name="mapper">Optional custom mapper (for DI/testing). If null, uses source-generated mapper.</param>
    /// <param name="partitionCacheDuration">Optional duration to cache partition discovery results (default: 5 minutes)</param>
    /// <param name="degreeOfParallelism">Degree of parallelism for reading (default: 0 = non-parallel)</param>
    public ParquetTable(
        IPartitionDiscoveryStrategy discoveryStrategy,
        IParquetReader reader,
        IParquetMapper<T>? mapper = null,
        TimeSpan? partitionCacheDuration = null,
        int degreeOfParallelism = 0)
    {
        mapper ??= ResolveMapper();
        _enumerationStrategy = new ParquetEnumerationStrategy<T>(
            discoveryStrategy, reader, mapper, partitionCacheDuration, degreeOfParallelism);
        
        Provider = new ParquetQueryProvider<T>(_enumerationStrategy);
        Expression = Expression.Constant(this);
    }

    private static IParquetMapper<T> ResolveMapper()
    {
        return ParquetMapperRegistry.Resolve<T>();
    }

    public Type ElementType => typeof(T);

    public Expression Expression { get; }

    public IQueryProvider Provider { get; }

    public IEnumerator<T> GetEnumerator()
    {
        return AsEnumerable().GetEnumerator();
    }

    IEnumerator IEnumerable.GetEnumerator()
    {
        return GetEnumerator();
    }

    public IQueryable<T> AsQueryable()
    {
        return this;
    }

    public IEnumerable<T> AsEnumerable()
    {
        return _enumerationStrategy.Enumerate();
    }

    public IEnumerable<Partition> DiscoverPartitions()
    {
        return _enumerationStrategy.DiscoverPartitions();
    }

    public void Dispose() { }
}
