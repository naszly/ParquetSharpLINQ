using System.Collections;

namespace ParquetSharpLINQ.Enumeration.Indexing;

public static class ComparerFactory
{
    public static IComparer<T> CreateDefault<T>()
    {
        return NullSafeComparer<T>.Create(Comparer<T>.Default);
    }

    public static IComparer<T> CreateGeneric<T>(IComparer<T> comparer)
    {
        return NullSafeComparer<T>.Create(comparer);
    }

    public static IComparer<T> CreateNonGeneric<T>(IComparer comparer)
    {
        return NullSafeComparer<T>.Create(new NonGenericComparerAdapter<T>(comparer));
    }

    private sealed class NonGenericComparerAdapter<T> : IComparer<T>
    {
        private readonly IComparer _comparer;

        public NonGenericComparerAdapter(IComparer comparer)
        {
            _comparer = comparer;
        }

        public int Compare(T? x, T? y)
        {
            return _comparer.Compare(x, y);
        }
    }

    private sealed class NullSafeComparer<T> : IComparer<T>
    {
        private readonly IComparer<T> _comparer;

        private NullSafeComparer(IComparer<T> comparer)
        {
            _comparer = comparer;
        }

        public static IComparer<T> Create(IComparer<T> comparer)
        {
            if (comparer is NullSafeComparer<T>)
                return comparer;

            return new NullSafeComparer<T>(comparer);
        }

        public int Compare(T? x, T? y)
        {
            if (x is null)
                return y is null ? 0 : -1;

            if (y is null)
                return 1;

            return _comparer.Compare(x, y);
        }
    }
}
