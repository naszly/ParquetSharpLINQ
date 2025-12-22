namespace ParquetSharpLINQ.ParquetSharp;

public readonly struct ParquetRow
{
    private readonly string[] _columnNames;
    private readonly IColumnBuffer[] _buffers;
    private readonly int _rowIndex;

    internal ParquetRow(string[] columnNames, IColumnBuffer[] buffers, int rowIndex)
    {
        _columnNames = columnNames;
        _buffers = buffers;
        _rowIndex = rowIndex;
    }

    public int ColumnCount => _buffers.Length;
    public ReadOnlySpan<string> ColumnNames => _columnNames;
    
    internal T GetValue<T>(int index)
    {
        return _buffers[index].GetValue<T>(_rowIndex);
    }

    internal IColumnBuffer[] Buffers => _buffers;

    internal int RowIndex => _rowIndex;
}
