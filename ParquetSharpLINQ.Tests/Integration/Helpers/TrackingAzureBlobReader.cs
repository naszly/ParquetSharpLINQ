using Azure.Storage.Blobs;
using ParquetSharpLINQ.Azure;

namespace ParquetSharpLINQ.Tests.Integration.Helpers;

/// <summary>
/// Test reader that tracks which Azure blobs are actually read.
/// Used to verify that range filters skip reading unnecessary blobs.
/// </summary>
public class TrackingAzureBlobReader(BlobContainerClient containerClient)
    : TrackingParquetReaderBase(new AzureBlobParquetReader(containerClient));
