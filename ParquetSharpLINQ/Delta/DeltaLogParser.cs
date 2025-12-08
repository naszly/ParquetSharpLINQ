using System.Text.Json;

namespace ParquetSharpLINQ.Delta;

public static class DeltaLogParser
{
    public static DeltaSnapshot BuildSnapshot(IEnumerable<DeltaAction> actions)
    {
        var activeFiles = new Dictionary<string, AddAction>(StringComparer.OrdinalIgnoreCase);
        MetadataAction? metadata = null;
        ProtocolAction? protocol = null;

        foreach (var action in actions)
        {
            switch (action)
            {
                case AddAction add:
                    activeFiles[add.Path] = add;
                    break;
                case RemoveAction remove:
                    activeFiles.Remove(remove.Path);
                    break;
                case MetadataAction meta:
                    metadata = meta;
                    break;
                case ProtocolAction proto:
                    protocol = proto;
                    break;
            }
        }

        return new DeltaSnapshot
        {
            ActiveFiles = activeFiles.Values.ToList(),
            Metadata = metadata,
            Protocol = protocol
        };
    }

    public static IEnumerable<DeltaAction> ParseActionsFromStream(StreamReader reader)
    {
        while (!reader.EndOfStream)
        {
            var line = reader.ReadLine();
            if (string.IsNullOrWhiteSpace(line))
            {
                continue;
            }

            var action = ParseAction(line);
            if (action != null)
            {
                yield return action;
            }
        }
    }

    public static DeltaAction? ParseAction(string json)
    {
        using var doc = JsonDocument.Parse(json);
        var root = doc.RootElement;

        if (root.TryGetProperty("add", out var addElement))
        {
            return new AddAction
            {
                Path = addElement.GetProperty("path").GetString() ?? string.Empty,
                PartitionValues = ParsePartitionValues(addElement),
                Size = addElement.TryGetProperty("size", out var size) ? size.GetInt64() : 0,
                ModificationTime = addElement.TryGetProperty("modificationTime", out var modTime) ? modTime.GetInt64() : 0,
                DataChange = addElement.TryGetProperty("dataChange", out var dataChange) && dataChange.GetBoolean()
            };
        }

        if (root.TryGetProperty("remove", out var removeElement))
        {
            return new RemoveAction
            {
                Path = removeElement.GetProperty("path").GetString() ?? string.Empty,
                DeletionTimestamp = removeElement.TryGetProperty("deletionTimestamp", out var delTime) ? delTime.GetInt64() : 0,
                DataChange = removeElement.TryGetProperty("dataChange", out var dataChange) && dataChange.GetBoolean()
            };
        }

        if (root.TryGetProperty("metaData", out var metaElement))
        {
            return new MetadataAction
            {
                Id = metaElement.GetProperty("id").GetString() ?? Guid.NewGuid().ToString(),
                Name = metaElement.TryGetProperty("name", out var name) ? name.GetString() : null,
                Description = metaElement.TryGetProperty("description", out var desc) ? desc.GetString() : null,
                PartitionColumns = ParsePartitionColumns(metaElement),
                CreatedTime = metaElement.TryGetProperty("createdTime", out var created) ? created.GetInt64() : 0
            };
        }

        if (root.TryGetProperty("protocol", out var protocolElement))
        {
            return new ProtocolAction
            {
                MinReaderVersion = protocolElement.GetProperty("minReaderVersion").GetInt32(),
                MinWriterVersion = protocolElement.GetProperty("minWriterVersion").GetInt32()
            };
        }

        return null;
    }

    private static Dictionary<string, string>? ParsePartitionValues(JsonElement element)
    {
        if (!element.TryGetProperty("partitionValues", out var partValues))
        {
            return null;
        }

        var result = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        foreach (var prop in partValues.EnumerateObject())
        {
            result[prop.Name] = prop.Value.GetString() ?? string.Empty;
        }

        return result.Count > 0 ? result : null;
    }

    private static List<string>? ParsePartitionColumns(JsonElement element)
    {
        if (!element.TryGetProperty("partitionColumns", out var partCols))
        {
            return null;
        }

        var result = new List<string>();
        foreach (var col in partCols.EnumerateArray())
        {
            var colName = col.GetString();
            if (!string.IsNullOrEmpty(colName))
            {
                result.Add(colName);
            }
        }

        return result.Count > 0 ? result : null;
    }

    public static long ExtractVersion(string fileName)
    {
        var name = Path.GetFileNameWithoutExtension(fileName);
        if (long.TryParse(name, out var version))
        {
            return version;
        }
        return -1;
    }
}

