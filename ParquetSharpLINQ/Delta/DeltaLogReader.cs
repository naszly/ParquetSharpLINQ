namespace ParquetSharpLINQ.Delta;

public class DeltaLogReader
{
    private readonly string _deltaLogPath;

    public DeltaLogReader(string tablePath)
    {
        _deltaLogPath = Path.Combine(tablePath, "_delta_log");
    }

    public DeltaSnapshot GetLatestSnapshot()
    {
        if (!Directory.Exists(_deltaLogPath))
        {
            throw new InvalidOperationException($"Not a Delta table. Missing _delta_log directory at {_deltaLogPath}");
        }

        var allActions = GetLogFiles().SelectMany(ReadLogFile);
        return DeltaLogParser.BuildSnapshot(allActions);
    }
    
    private IEnumerable<string> GetLogFiles()
    {
        var jsonFiles = Directory
            .GetFiles(_deltaLogPath, "*.json")
            .OrderBy(DeltaLogParser.ExtractVersion);

        foreach (var file in jsonFiles)
        {
            yield return file;
        }
    }

    private static IEnumerable<DeltaAction> ReadLogFile(string filePath)
    {
        foreach (var line in File.ReadLines(filePath))
        {
            if (string.IsNullOrWhiteSpace(line))
            {
                continue;
            }

            var action = DeltaLogParser.ParseAction(line);
            if (action != null)
            {
                yield return action;
            }
        }
    }
}


