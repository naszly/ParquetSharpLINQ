using ParquetSharp;

namespace ParquetSharpLINQ;

public interface IParquetReader
{
    IEnumerable<string> ListFiles(string directory);

    IEnumerable<Column> GetColumns(string filePath);

    IEnumerable<Dictionary<string, object?>> ReadRows(string filePath, IEnumerable<string> columns);
}