namespace ParquetSharpLINQ.ParquetSharp.Buffers;

internal interface IColumnBuffer
{
    TTarget GetValue<TTarget>(int index);
}
