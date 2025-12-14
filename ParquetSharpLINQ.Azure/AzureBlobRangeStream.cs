using Azure;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;

namespace ParquetSharpLINQ.Azure;

internal sealed class AzureBlobRangeStream : Stream
{
    private readonly BlobClient _blob;

    public AzureBlobRangeStream(BlobClient blob, long length)
    {
        _blob = blob;
        Length = length;
    }

    public override bool CanRead => true;
    public override bool CanSeek => true;
    public override bool CanWrite => false;
    public override long Length { get; }
    public override long Position { get; set; }

    public override long Seek(long offset, SeekOrigin origin)
    {
        Position = origin switch
        {
            SeekOrigin.Begin => offset,
            SeekOrigin.Current => Position + offset,
            SeekOrigin.End => Length + offset,
            _ => throw new ArgumentOutOfRangeException(nameof(origin))
        };
        return Position;
    }

    public override int Read(byte[] buffer, int offset, int count)
    {
        if (Position >= Length)
            return 0;

        var toRead = (int)Math.Min(count, Length - Position);

        var options = new BlobDownloadOptions
        {
            Range = new HttpRange(Position, toRead)
        };
        var response = _blob.DownloadStreaming(options);

        using var stream = response.Value.Content;
        var read = stream.Read(buffer, offset, toRead);

        Position += read;
        return read;
    }

    // Required overrides
    public override void Flush() { }
    public override void SetLength(long value) => throw new NotSupportedException();
    public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
}