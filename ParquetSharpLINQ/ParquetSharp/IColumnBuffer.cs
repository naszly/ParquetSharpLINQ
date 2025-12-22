namespace ParquetSharpLINQ.ParquetSharp;

internal interface IColumnBuffer
{
    TTarget GetValue<TTarget>(int index);
}
