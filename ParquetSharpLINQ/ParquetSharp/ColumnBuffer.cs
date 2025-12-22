using System.Collections.Immutable;

namespace ParquetSharpLINQ.ParquetSharp;

internal sealed class ColumnBuffer<T>(ImmutableArray<T> values) : IColumnBuffer
{
    public TTarget GetValue<TTarget>(int index)
    {
        var value = values[index];
        return ColumnBufferConverter.Convert<T, TTarget>(value);
    }
}
