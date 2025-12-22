using ParquetSharpLINQ.Interfaces;

namespace ParquetSharpLINQ.Mappers;

public static class ParquetMapperRegistry
{
    private static readonly object Lock = new();
    private static readonly Dictionary<Type, Func<object>> MapperFactories = new();
    private static readonly Dictionary<Type, IParquetTypeMetadata> MetadataRegistry = new();

    public static void Register(Type type, Func<object> mapperFactory, IParquetTypeMetadata metadata)
    {
        if (type == null) throw new ArgumentNullException(nameof(type));
        if (mapperFactory == null) throw new ArgumentNullException(nameof(mapperFactory));
        if (metadata == null) throw new ArgumentNullException(nameof(metadata));

        lock (Lock)
        {
            MapperFactories[type] = mapperFactory;
            MetadataRegistry[type] = metadata;
        }
    }

    public static IParquetMapper<T> Resolve<T>() where T : new()
    {
        lock (Lock)
        {
            if (MapperFactories.TryGetValue(typeof(T), out var factory))
                return (IParquetMapper<T>)factory();
        }

        throw new InvalidOperationException(
            $"No source-generated mapper found for type {typeof(T).FullName}. " +
            "Make sure the type has [ParquetColumn] attributes and the project is built.");
    }

    public static bool TryGetMetadata(Type type, out IParquetTypeMetadata? metadata)
    {
        lock (Lock)
        {
            if (MetadataRegistry.TryGetValue(type, out var found))
            {
                metadata = found;
                return true;
            }
        }

        metadata = null;
        return false;
    }
}
