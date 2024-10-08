using System.Buffers;
using System.IO.Pipelines;
using LunaDB.Buffers;
using Microsoft.Win32.SafeHandles;

namespace LunaDB;

public sealed class Database(SafeFileHandle fileHandle, SafeFileHandle indexHandle) : IDisposable
{
    public static Database Open(string path)
    {
        File.Delete(path);
        File.Delete("idx");

        return new Database(
            File.OpenHandle(
                path: path,
                mode: FileMode.CreateNew,
                access: FileAccess.ReadWrite,
                share: FileShare.None,
                options: FileOptions.Asynchronous
            ),
            File.OpenHandle(
                path: "idx",
                mode: FileMode.CreateNew,
                access: FileAccess.ReadWrite,
                share: FileShare.None,
                options: FileOptions.Asynchronous
            )
        );
    }

    public void Dispose()
    {
        fileHandle.Close();
        fileHandle.Dispose();
    }

    public long WriteIndex(int documentId, long documentOffset, long indexOffset)
    {
        // file layout
        // |-------------|----------------------|
        // | id (int 32) | data offset (int 64) |
        // |-------------|----------------------|

        var buffer = new byte[sizeof(int) + sizeof(long)].AsSpan();

        BinaryPrimitives.WriteInt32(buffer[..4], documentId);
        BinaryPrimitives.WriteInt64(buffer[4..], documentOffset);

        RandomAccess.Write(indexHandle, buffer, indexOffset);

        return indexOffset + buffer.Length;
    }

    public long WriteDocument(int id, ReadOnlySpan<byte> data, long offset)
    {
        // file layout
        // |-------------|-----------------|----------------------|-----------------------|
        // | id (int 32) | tombstone (bit) | data length (int 16) | data (max 1 Kibibyte) |
        // |-------------|-----------------|----------------------|-----------------------|

        const int OneKibibyte = 1024;

        if (data.Length > OneKibibyte)
            throw new ArgumentException("Data would be truncated.");

        var buffer = new byte[sizeof(int) + sizeof(short) + sizeof(byte) + data.Length].AsSpan();

        BinaryPrimitives.WriteInt32(buffer[..4], id);
        buffer[4] = 0;
        BinaryPrimitives.WriteInt16(buffer[5..7], (short)data.Length);
        data.CopyTo(buffer[7..]);

        RandomAccess.Write(fileHandle, buffer, offset);

        return offset + buffer.Length;
    }

    public void FlushToDisk()
    {
        RandomAccess.FlushToDisk(fileHandle);
        RandomAccess.FlushToDisk(indexHandle);
    }

    public Task DeleteAsync(int documentId)
    {
        return TombstoneAsync(documentId);
    }

    private async Task TombstoneAsync(int documentId)
    {
        var indexEntryBuffer = new byte[8];

        _ = await RandomAccess
            .ReadAsync(indexHandle, indexEntryBuffer, 12 * (documentId - 1) + 4)
            .ConfigureAwait(false);

        var documentBuffer = new byte[1];
        var offset = BinaryPrimitives.ReadInt64(indexEntryBuffer.AsSpan());

        _ = await RandomAccess
            .ReadAsync(fileHandle, documentBuffer, offset + 4)
            .ConfigureAwait(false);

        // bail if tombstone bit is already set
        if (documentBuffer[0] != 0)
            return;

        // set the tombstone bit to 1
        documentBuffer[0] = 1;

        await RandomAccess
            .WriteAsync(fileHandle, documentBuffer, offset + 4)
            .ConfigureAwait(false);

        var zero = new byte[sizeof(int)];

        await RandomAccess
            .WriteAsync(indexHandle, zero, 12 * (documentId - 1))
            .ConfigureAwait(false);
    }

    public async Task<Document?> FindByIdAsync(int documentId)
    {
        var documentOffsetMemory = new byte[12].AsMemory();

        _ = await RandomAccess
            .ReadAsync(indexHandle, documentOffsetMemory, 12 * (documentId - 1))
            .ConfigureAwait(false);

        if (BinaryPrimitives.UnsafeIsInt32EqualToZero(documentOffsetMemory[..4].Span))
            return null;

        var offset = BinaryPrimitives.ReadInt64(documentOffsetMemory[4..].Span);
        var documentBuffer = new byte[4 + 1 + 2 + 1024];

        _ = await RandomAccess
            .ReadAsync(fileHandle, documentBuffer, offset);

        var wrapper = new ReadOnlySequence<byte>(documentBuffer);

        TryReadDocument(ref wrapper, out var document);

        return document;
    }

    public async IAsyncEnumerable<Document> ScanAsync()
    {
        var pipe = new Pipe();

        var writing = FillPipeAsync(pipe.Writer);

        await foreach (var document in ReadFromPipeAsync(pipe.Reader))
            yield return document;

        await writing.ConfigureAwait(false);
    }

    private async Task FillPipeAsync(PipeWriter writer)
    {
        var totalBytesRead = 0L;

        while (true)
        {
            var memory = writer.GetMemory(64 * 1024);

            var bytesRead = (int)
                await RandomAccess
                    .ReadAsync(fileHandle, memory, totalBytesRead)
                    .ConfigureAwait(false);

            if (bytesRead == 0)
                break;

            totalBytesRead += bytesRead;

            writer.Advance(bytesRead);

            var result = await writer.FlushAsync().ConfigureAwait(false);

            if (result.IsCompleted)
                break;
        }

        await writer.CompleteAsync().ConfigureAwait(false);
    }

    private static async IAsyncEnumerable<Document> ReadFromPipeAsync(PipeReader reader)
    {
        while (true)
        {
            var result = await reader.ReadAsync().ConfigureAwait(false);
            var buffer = result.Buffer;

            while (TryReadDocument(ref buffer, out var document))
                yield return document!;

            reader.AdvanceTo(buffer.Start, buffer.End);

            if (result.IsCompleted)
                break;
        }

        await reader.CompleteAsync().ConfigureAwait(false);
    }

    private static bool TryReadDocument(ref ReadOnlySequence<byte> buffer, out Document? document)
    {
        var reader = new SequenceReader<byte>(buffer);

        if (
            !reader.TryReadLittleEndian(out int id) ||
            !(reader.TryRead(out var tombstone) && tombstone == 0) ||
            !reader.TryReadLittleEndian(out short length)
        )
        {
            document = null;
            return false;
        }

        // Check if the buffer contains the entire record
        if (reader.Remaining < length)
        {
            document = null;
            return false;
        }

        // Read the record data
        var recordSlice = buffer.Slice(reader.Position, length);

        document = new Document(Id: id, Data: recordSlice.First);

        // Move the buffer past the record
        buffer = buffer.Slice(recordSlice.End);

        return true;
    }
}