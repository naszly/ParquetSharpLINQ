using System.Collections.Immutable;
using ParquetSharpLINQ.Common.Converter;

namespace ParquetSharpLINQ.ParquetSharp.Buffers;

internal sealed class ColumnBuffer<T>(ImmutableArray<T> values) : IColumnBuffer
{
    public TTarget GetValue<TTarget>(int index)
    {
        var value = values[index];
        return ColumnValueConverter.Convert<T, TTarget>(value);
    }
}
