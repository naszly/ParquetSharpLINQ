using ParquetSharpLINQ.ParquetSharp.Buffers;
using ParquetSharpLINQ.ParquetSharp.ParquetRow;

namespace ParquetSharpLINQ.Tests.Helpers;

internal static class ParquetRowFactory
{
    public static (string Name, IColumnBuffer Buffer) Column<T>(string name, T value)
    {
        return (name, new ConstantColumnBuffer<T>(value));
    }

    public static ParquetRow Create(params (string Name, IColumnBuffer Buffer)[] columns)
    {
        var names = new string[columns.Length];
        var buffers = new IColumnBuffer[columns.Length];

        for (var i = 0; i < columns.Length; i++)
        {
            names[i] = columns[i].Name;
            buffers[i] = columns[i].Buffer;
        }

        return new ParquetRow(names, buffers, 0);
    }
}
