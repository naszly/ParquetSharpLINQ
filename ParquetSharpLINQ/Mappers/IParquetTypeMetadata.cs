using System.Collections.Immutable;
using ParquetSharpLINQ.Enumeration.Indexing;

namespace ParquetSharpLINQ.Mappers;

public interface IParquetTypeMetadata
{
    IImmutableDictionary<string, string> PropertyToColumnMap { get; }

    IImmutableSet<string> PartitionPropertyNames { get; }

    IImmutableDictionary<string, IIndexedColumnDefinition> IndexedColumnDefinitions { get; }

    IReadOnlyDictionary<string, Delegate> PropertyAccessors { get; }
}
