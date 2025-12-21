namespace ParquetSharpLINQ.Enumeration.Indexing;

#if !NET9_0_OR_GREATER
using Lock = object;
#endif

internal sealed class IndexedColumnIndexStore
{
    private readonly Lock _lock = new();
    private readonly Dictionary<string, IndexedColumnIndex> _columns =
        new(StringComparer.OrdinalIgnoreCase);

    public IndexedColumnIndex GetOrAddColumn(string columnName)
    {
        lock (_lock)
        {
            if (_columns.TryGetValue(columnName, out var columnIndex))
            {
                return columnIndex;
            }
            
            columnIndex = new IndexedColumnIndex();
            _columns[columnName] = columnIndex;

            return columnIndex;
        }
    }

    public void Clear()
    {
        lock (_lock)
        {
            _columns.Clear();
        }
    }
}

internal sealed class IndexedColumnIndex
{
    private readonly Lock _lock = new();
    private readonly Dictionary<string, RowGroupIndex> _files =
        new(StringComparer.OrdinalIgnoreCase);

    public RowGroupIndex GetOrAddFile(string filePath, Func<RowGroupIndex> factory)
    {
        lock (_lock)
        {
            if (_files.TryGetValue(filePath, out var index))
                return index;

            index = factory();
            _files[filePath] = index;
            return index;
        }
    }
}

internal sealed class RowGroupIndex
{
    public RowGroupIndex(IReadOnlyDictionary<int, SortedValueArray> rowGroups)
    {
        RowGroups = rowGroups;
    }

    public IReadOnlyDictionary<int, SortedValueArray> RowGroups { get; }
}

internal sealed class SortedValueArray
{
    private readonly object?[] _values;
    private readonly IComparer<object?> _comparer;

    public SortedValueArray(IEnumerable<object?> values, IComparer<object?> comparer)
    {
        _comparer = comparer;
        _values = values.ToArray();
        Array.Sort(_values, _comparer);
    }

    public int Count => _values.Length;

    public object? Min => _values.Length > 0 ? _values[0] : null;

    public object? Max => _values.Length > 0 ? _values[^1] : null;

    public bool Contains(object? value)
    {
        return BinarySearch(value) >= 0;
    }

    public bool HasAnyValueNotEqual(object? value)
    {
        if (_values.Length == 0)
            return false;

        return _comparer.Compare(Min, value) != 0 || _comparer.Compare(Max, value) != 0;
    }

    public bool IntersectsRange(object? minValue, bool minInclusive, object? maxValue, bool maxInclusive)
    {
        if (_values.Length == 0)
            return false;

        if (minValue != null)
        {
            var maxCompare = _comparer.Compare(Max, minValue);
            if (maxCompare < 0 || (maxCompare == 0 && !minInclusive))
                return false;
        }

        if (maxValue != null)
        {
            var minCompare = _comparer.Compare(Min, maxValue);
            if (minCompare > 0 || (minCompare == 0 && !maxInclusive))
                return false;
        }

        return true;
    }

    public int CountEquals(object? value)
    {
        if (_values.Length == 0)
            return 0;

        var lower = LowerBound(value, true);
        var upper = UpperBoundExclusive(value, true);
        return Math.Max(0, upper - lower);
    }

    public int CountInRange(object? minValue, bool minInclusive, object? maxValue, bool maxInclusive)
    {
        if (_values.Length == 0)
            return 0;

        var lower = minValue == null ? 0 : LowerBound(minValue, minInclusive);
        var upper = maxValue == null ? _values.Length : UpperBoundExclusive(maxValue, maxInclusive);
        return Math.Max(0, upper - lower);
    }

    private int BinarySearch(object? value)
    {
        return Array.BinarySearch(_values, value, _comparer);
    }

    private int LowerBound(object? value, bool inclusive)
    {
        var lo = 0;
        var hi = _values.Length;
        while (lo < hi)
        {
            var mid = lo + ((hi - lo) / 2);
            var compare = _comparer.Compare(_values[mid], value);
            if (compare < 0 || (compare == 0 && !inclusive))
            {
                lo = mid + 1;
            }
            else
            {
                hi = mid;
            }
        }

        return lo;
    }

    private int UpperBoundExclusive(object? value, bool inclusive)
    {
        var lo = 0;
        var hi = _values.Length;
        while (lo < hi)
        {
            var mid = lo + ((hi - lo) / 2);
            var compare = _comparer.Compare(_values[mid], value);
            if (compare < 0 || (compare == 0 && inclusive))
            {
                lo = mid + 1;
            }
            else
            {
                hi = mid;
            }
        }

        return lo;
    }
}
