using LunaDB.Buffers;
using Microsoft.Win32.SafeHandles;

namespace LunaDB;

public sealed class Database(SafeFileHandle fileHandle) : IDisposable
{
    public static Database Open(string path)
    {
        return new Database(
            File.OpenHandle(
                path: path,
                mode: FileMode.Create,
                access: FileAccess.ReadWrite,
                share: FileShare.None,
                options: FileOptions.None,
                preallocationSize: 64 * 1024
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

    public record Document(int Id, Memory<byte> Data);

    public IEnumerable<Document> Scan()
    {
        const int SixtyFourKibibytes = 64 * 1024;

        var buffer = new byte[SixtyFourKibibytes];

        var bytesRead = RandomAccess.Read(fileHandle, [buffer], 0);

        Console.WriteLine("loaded " + bytesRead + " bytes into 64 KiB buffer");

        var slice = buffer.AsMemory(0, (int) bytesRead);
        var consumedBytes = 0;

        while (consumedBytes < bytesRead)
        {
            var documentSlice = slice[consumedBytes..];

            var document = ReadDocument(documentSlice);
            consumedBytes += 4 + 2 + document.Data.Length;

            yield return document;

            Console.WriteLine("consumed " + consumedBytes + " bytes");
        }
    }

    private static Document ReadDocument(Memory<byte> documentSlice)
    {
        var span = documentSlice.Span;
        var id = BinaryPrimitives.ReadInt32(span[..4]);
        var dataLength = BinaryPrimitives.ReadInt16(span[4..6]);
        var data = documentSlice.Slice(6, dataLength);
        return new Document(id, data);
    }
}
