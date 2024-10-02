using System.Buffers;
using System.Data.Common;
using LunaDB.Buffers;
using Microsoft.Win32.SafeHandles;
using System.IO.Pipelines;

namespace LunaDB;

public sealed class Database(SafeFileHandle fileHandle) : IDisposable
{
    public static Database Open(string path)
    {
        File.Delete(path);

        return new Database(
            File.OpenHandle(
                path: path,
                mode: FileMode.CreateNew,
                access: FileAccess.ReadWrite,
                share: FileShare.None,
                options: FileOptions.Asynchronous
            )
        );
    }

    public void Dispose()
    {
        GC.SuppressFinalize(this);

        fileHandle.Close();
        fileHandle.Dispose();
    }

    public long WriteDocument(int id, ReadOnlySpan<byte> data, long offset)
    {
        // file layout
        // |-------------|----------------------|-----------------------|
        // | id (int 32) | data length (int 16) | data (max 1 Kibibyte) |
        // |-------------|----------------------|-----------------------|

        const int OneKibibyte = 1024;

        if (data.Length > OneKibibyte)
            throw new ArgumentException("Data would be truncated.");

        var buffer = new byte[sizeof(int) + sizeof(short) + data.Length].AsSpan();

        BinaryPrimitives.WriteInt32(buffer[..4], id);
        BinaryPrimitives.WriteInt16(buffer[4..6], (short)data.Length);
        data.CopyTo(buffer[6..]);

        RandomAccess.Write(fileHandle, buffer, offset);

        return offset + buffer.Length;
    }

    public void FlushToDisk()
    {
        RandomAccess.FlushToDisk(fileHandle);
    }

    public record Document(int Id, ReadOnlyMemory<byte> Data);

    private async Task FillPipeAsync(PipeWriter writer)
    {
        var totalBytesRead = 0L;

        while (true)
        {
            var memory = writer.GetMemory(64 * 1024);

            var bytesRead = (int) await RandomAccess.ReadAsync(fileHandle, [memory], totalBytesRead);
            
            if (bytesRead == 0)
                break;

            totalBytesRead += bytesRead;
            
            writer.Advance(bytesRead);

            var result = await writer.FlushAsync();

            if (result.IsCompleted)
                break;
        }

        await writer.CompleteAsync();
    }

    private async IAsyncEnumerable<Document> ReadFromPipe(PipeReader reader)
    {
        while (true)
        {
            var result = await reader.ReadAsync();
            var buffer = result.Buffer;

            while (TryReadDocument(ref buffer, out var document))
                yield return document!;

            reader.AdvanceTo(buffer.Start, buffer.End);

            if (result.IsCompleted)
            {
                break;
            }
        }

        await reader.CompleteAsync();
    }

    private static bool TryReadDocument(ref ReadOnlySequence<byte> buffer, out Document? document)
    {
        var reader = new SequenceReader<byte>(buffer);

        if (!reader.TryReadLittleEndian(out int id) ||
            !reader.TryReadLittleEndian(out short length))
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

    public async IAsyncEnumerable<Document> ScanAsync()
    {
        var pipe = new Pipe();

        var writing = FillPipeAsync(pipe.Writer);

        await foreach (var document in ReadFromPipe(pipe.Reader))
            yield return document;

        await writing;
    }
}