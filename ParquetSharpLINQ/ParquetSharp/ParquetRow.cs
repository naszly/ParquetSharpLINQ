namespace ParquetSharpLINQ.ParquetSharp;

public readonly struct ParquetRow
{
    private readonly string[] _columnNames;
    private readonly object?[] _values;

    public ParquetRow(string[] columnNames, object?[] values)
    {
        _columnNames = columnNames;
        _values = values;
    }

    public object? this[int index] => _values[index];
    public int ColumnCount => _values.Length;
    public ReadOnlySpan<string> ColumnNames => _columnNames;
    public ReadOnlySpan<object?> Values => _values;
}
