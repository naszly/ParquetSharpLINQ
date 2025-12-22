using System.Collections.Immutable;
using ParquetSharp;
using ParquetSharpLINQ.Interfaces;
using ParquetSharpLINQ.ParquetSharp;

namespace ParquetSharpLINQ.Tests.Integration.Helpers;

public abstract class TrackingParquetReaderBase : IParquetReader
{
    private readonly IParquetReader _innerReader;
    private readonly HashSet<string> _filesRead = [];
    private readonly Dictionary<string, Dictionary<string, int>> _columnReadCounts =
        new(StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<string, int> _indexReadCounts =
        new(StringComparer.OrdinalIgnoreCase);
    private readonly object _lock = new();

    protected TrackingParquetReaderBase(IParquetReader innerReader)
    {
        _innerReader = innerReader ?? throw new ArgumentNullException(nameof(innerReader));
    }

    public IReadOnlySet<string> FilesRead => _filesRead;

    public int GetIndexReadCount(string columnName)
    {
        lock (_lock)
        {
            return _indexReadCounts.TryGetValue(columnName, out var count) ? count : 0;
        }
    }

    public IEnumerable<Column> GetColumns(string filePath)
    {
        return _innerReader.GetColumns(filePath);
    }

    public IEnumerable<ParquetRow> ReadRows(
        string filePath,
        IEnumerable<string> columns,
        IReadOnlySet<int>? rowGroupsToRead = null)
    {
        var columnList = columns.ToList();

        lock (_lock)
        {
            var fileName = Path.GetFileName(filePath);
            _filesRead.Add(fileName);

            if (!_columnReadCounts.TryGetValue(fileName, out var perFile))
            {
                perFile = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
                _columnReadCounts[fileName] = perFile;
            }

            foreach (var column in columnList.Distinct(StringComparer.OrdinalIgnoreCase))
            {
                perFile[column] = perFile.TryGetValue(column, out var count) ? count + 1 : 1;
            }
        }

        return _innerReader.ReadRows(filePath, columnList, rowGroupsToRead);
    }

    public IReadOnlyList<ImmutableArray<T>> ReadColumnValuesByRowGroup<T>(string filePath, string columnName)
    {
        lock (_lock)
        {
            var fileName = Path.GetFileName(filePath);
            _filesRead.Add(fileName);

            if (!_columnReadCounts.TryGetValue(fileName, out var perFile))
            {
                perFile = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
                _columnReadCounts[fileName] = perFile;
            }

            perFile[columnName] = perFile.TryGetValue(columnName, out var count) ? count + 1 : 1;

            _indexReadCounts[columnName] = _indexReadCounts.TryGetValue(columnName, out var total)
                ? total + 1
                : 1;
        }

        return _innerReader.ReadColumnValuesByRowGroup<T>(filePath, columnName);
    }
}
