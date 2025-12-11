using ParquetSharp;
using ParquetSharpLINQ.ParquetSharp;

namespace ParquetSharpLINQ;

public interface IParquetReader
{
    IEnumerable<Column> GetColumns(string filePath);

    IEnumerable<ParquetRow> ReadRows(string filePath, IEnumerable<string> columns);
}