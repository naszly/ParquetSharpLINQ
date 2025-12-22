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

public sealed class RowGroupIndex
{
    public RowGroupIndex(IReadOnlyDictionary<int, RowGroupValues> rowGroups)
    {
        RowGroups = rowGroups;
    }

    public IReadOnlyDictionary<int, RowGroupValues> RowGroups { get; }
}

public abstract class RowGroupValues
{
    public abstract Type ValueType { get; }
}

public sealed class RowGroupValues<T> : RowGroupValues
{
    public RowGroupValues(SortedValueArray<T> values)
    {
        Values = values;
    }

    public override Type ValueType => typeof(T);

    public SortedValueArray<T> Values { get; }
}

public sealed class SortedValueArray<T>
{
    private readonly T[] _values;
    private readonly IComparer<T?> _comparer;

    public SortedValueArray(IEnumerable<T> values, IComparer<T> comparer)
    {
        _comparer = comparer;
        _values = values.ToArray();
        Array.Sort(_values, _comparer);
    }

    public int Count => _values.Length;

    public T Min => _values[0];

    public T Max => _values[^1];

    public bool Contains(T? value)
    {
        return BinarySearch(value) >= 0;
    }

    public bool HasAnyValueNotEqual(T? value)
    {
        if (_values.Length == 0)
            return false;

        return _comparer.Compare(Min, value) != 0 || _comparer.Compare(Max, value) != 0;
    }

    public bool IntersectsRange(T? minValue, bool hasMin, bool minInclusive, T? maxValue, bool hasMax,
        bool maxInclusive)
    {
        if (_values.Length == 0)
            return false;

        if (hasMin)
        {
            var maxCompare = _comparer.Compare(Max, minValue);
            if (maxCompare < 0 || (maxCompare == 0 && !minInclusive))
                return false;
        }

        if (hasMax)
        {
            var minCompare = _comparer.Compare(Min, maxValue);
            if (minCompare > 0 || (minCompare == 0 && !maxInclusive))
                return false;
        }

        return true;
    }

    public int CountEquals(T? value)
    {
        if (_values.Length == 0)
            return 0;

        var lower = LowerBound(value, true);
        var upper = UpperBoundExclusive(value, true);
        return Math.Max(0, upper - lower);
    }

    public int CountInRange(T? minValue, bool hasMin, bool minInclusive, T? maxValue, bool hasMax,
        bool maxInclusive)
    {
        if (_values.Length == 0)
            return 0;

        var lower = hasMin ? LowerBound(minValue, minInclusive) : 0;
        var upper = hasMax ? UpperBoundExclusive(maxValue, maxInclusive) : _values.Length;
        return Math.Max(0, upper - lower);
    }

    private int BinarySearch(T? value)
    {
        return Array.BinarySearch(_values, value, _comparer);
    }

    private int LowerBound(T? value, bool inclusive)
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

    private int UpperBoundExclusive(T? value, bool inclusive)
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
