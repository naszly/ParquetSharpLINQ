using ParquetSharp;

namespace ParquetSharpLINQ;

public interface IParquetReader
{
    IEnumerable<Column> GetColumns(string filePath);

    IEnumerable<Dictionary<string, object?>> ReadRows(string filePath, IEnumerable<string> columns);
}