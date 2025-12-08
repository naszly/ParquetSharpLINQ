namespace ParquetSharpLINQ.Delta;

public class DeltaSnapshot
{
    public List<AddAction> ActiveFiles { get; init; } = [];
    public MetadataAction? Metadata { get; init; }
    public ProtocolAction? Protocol { get; init; }
}

