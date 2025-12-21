using ParquetSharpLINQ.ParquetSharp;

namespace ParquetSharpLINQ.Tests.Integration.Helpers;

/// <summary>
/// Test reader that tracks which files are actually read.
/// Used to verify that range filters skip reading unnecessary files.
/// </summary>
public class TrackingParquetReader() : TrackingParquetReaderBase(new ParquetSharpReader());
