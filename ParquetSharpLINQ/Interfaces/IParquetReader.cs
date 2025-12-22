using System.Collections.Immutable;
using ParquetSharp;
using ParquetSharpLINQ.ParquetSharp;

namespace ParquetSharpLINQ.Interfaces;

public interface IParquetReader
{
    IEnumerable<Column> GetColumns(string filePath);

    IEnumerable<ParquetRow> ReadRows(
        string filePath,
        IEnumerable<string> columns,
        IReadOnlySet<int>? rowGroupsToRead = null);

    IReadOnlyList<ImmutableArray<T>> ReadColumnValuesByRowGroup<T>(string filePath, string columnName);
}
