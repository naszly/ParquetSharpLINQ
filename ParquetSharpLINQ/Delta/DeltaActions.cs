namespace ParquetSharpLINQ.Delta;

public abstract class DeltaAction { }

public class AddAction : DeltaAction
{
    public required string Path { get; init; }
    public Dictionary<string, string>? PartitionValues { get; init; }
    public long Size { get; init; }
    public long ModificationTime { get; init; }
    public bool DataChange { get; init; }
    public Dictionary<string, string>? Stats { get; init; }
}

public class RemoveAction : DeltaAction
{
    public required string Path { get; init; }
    public long DeletionTimestamp { get; init; }
    public bool DataChange { get; init; }
}

public class MetadataAction : DeltaAction
{
    public required string Id { get; init; }
    public string? Name { get; init; }
    public string? Description { get; init; }
    public List<string>? PartitionColumns { get; init; }
    public long CreatedTime { get; init; }
}

public class ProtocolAction : DeltaAction
{
    public int MinReaderVersion { get; init; }
    public int MinWriterVersion { get; init; }
}

