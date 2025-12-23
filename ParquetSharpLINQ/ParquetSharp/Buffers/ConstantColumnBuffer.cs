using ParquetSharpLINQ.Common.Converter;

namespace ParquetSharpLINQ.ParquetSharp.Buffers;

internal sealed class ConstantColumnBuffer<T>(T value) : IColumnBuffer
{
    public TTarget GetValue<TTarget>(int index)
    {
        return ColumnValueConverter.Convert<T, TTarget>(value);
    }
}
