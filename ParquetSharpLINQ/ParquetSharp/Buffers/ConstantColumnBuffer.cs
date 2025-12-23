using ParquetSharpLINQ.ParquetSharp.Buffers.Converter;

namespace ParquetSharpLINQ.ParquetSharp.Buffers;

internal sealed class ConstantColumnBuffer<T>(T value) : IColumnBuffer
{
    public TTarget GetValue<TTarget>(int index)
    {
        return ColumnBufferConverter.Convert<T, TTarget>(value);
    }
}
