using ParquetSharp;

namespace ParquetSharpLINQ.ParquetSharp;

/// <summary>
/// Resolves CLR types from Parquet column descriptors.
/// </summary>
internal static class ParquetTypeResolver
{
    /// <summary>
    /// Resolves the appropriate CLR type for a Parquet column descriptor.
    /// </summary>
    public static Type ResolveClrType(ColumnDescriptor descriptor)
    {
        var logicalType = descriptor.LogicalType;

        return logicalType switch
        {
            IntLogicalType intType => ResolveIntType(intType),
            DecimalLogicalType => typeof(decimal),
            StringLogicalType => typeof(string),
            DateLogicalType => typeof(DateOnly),
            TimeLogicalType => typeof(TimeSpan),
            TimestampLogicalType => typeof(DateTime),
            _ => ResolvePhysicalType(descriptor.PhysicalType)
        };
    }

    /// <summary>
    /// Determines if a column should use a nullable type.
    /// </summary>
    public static bool ShouldUseNullable(ColumnDescriptor descriptor)
    {
        return descriptor.MaxRepetitionLevel > 0 || descriptor.MaxDefinitionLevel > 0;
    }

    private static Type ResolveIntType(IntLogicalType intType)
    {
        return intType.BitWidth switch
        {
            8 => intType.IsSigned ? typeof(sbyte) : typeof(byte),
            16 => intType.IsSigned ? typeof(short) : typeof(ushort),
            32 => intType.IsSigned ? typeof(int) : typeof(uint),
            64 => intType.IsSigned ? typeof(long) : typeof(ulong),
            _ => throw new NotSupportedException($"Int bit width {intType.BitWidth} is not supported")
        };
    }

    private static Type ResolvePhysicalType(PhysicalType physicalType)
    {
        return physicalType switch
        {
            PhysicalType.Boolean => typeof(bool),
            PhysicalType.Int32 => typeof(int),
            PhysicalType.Int64 => typeof(long),
            PhysicalType.Float => typeof(float),
            PhysicalType.Double => typeof(double),
            PhysicalType.ByteArray => typeof(byte[]),
            PhysicalType.FixedLenByteArray => typeof(byte[]),
            _ => throw new NotSupportedException($"Physical type {physicalType} is not supported")
        };
    }
}

