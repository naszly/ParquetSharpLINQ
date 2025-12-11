using ParquetSharpLINQ.ParquetSharp;

namespace ParquetSharpLINQ;

public interface IParquetMapper<out T>
{
    IReadOnlyList<string>? RequiredColumns { get; }

    T Map(ParquetRow row);
}