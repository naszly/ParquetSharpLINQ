namespace ParquetSharpLINQ;

public interface IParquetMapper<out T>
{
    IReadOnlyList<string>? RequiredColumns { get; }

    T Map(IReadOnlyDictionary<string, object?> row);
}