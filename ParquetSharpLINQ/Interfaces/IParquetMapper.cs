using ParquetSharpLINQ.ParquetSharp;

namespace ParquetSharpLINQ.Interfaces;

public interface IParquetMapper<out T>
{
    IReadOnlyList<string>? RequiredColumns { get; }

    T Map(ParquetRow row);

    T Map(ParquetRow row, IReadOnlyCollection<string>? requestedColumns);
}
