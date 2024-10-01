using LunaDB.Buffers;
using Microsoft.Win32.SafeHandles;

namespace LunaDB;

public sealed class Database : IDisposable
{
    private readonly SafeFileHandle _fileHandle;

    public static Database OpenDatabase(string path)
    {
        return new Database(
            File.OpenHandle(
                path: "db",
                mode: FileMode.Create,
                access: FileAccess.Write,
                share: FileShare.None,
                options: FileOptions.None
            )
        );
    }

    public Database(SafeFileHandle fileHandle)
    {
        _fileHandle = fileHandle;
    }

    public void Dispose()
    {
        GC.SuppressFinalize(this);

        _fileHandle.Close();
        _fileHandle.Dispose();
    }

    public void WriteDocument(int id, ReadOnlySpan<byte> data, long offset)
    {
        // file layout
        // |-------------|----------------------|-----------------------|
        // | id (int 32) | data length (int 16) | data (max 1 Kibibyte) |
        // |-------------|----------------------|-----------------------|

        const int OneKibibyte = 1024;

        if (data.Length > OneKibibyte)
            throw new ArgumentException("Data would be truncated.");

        var buffer = new byte[sizeof(int) + sizeof(short) + data.Length].AsSpan();

        BinaryPrimitives.WriteInt32(buffer[0..4], id);
        BinaryPrimitives.WriteInt16(buffer[4..6], (short)data.Length);
        data.CopyTo(buffer[6..]);

        RandomAccess.Write(_fileHandle, buffer, offset);
    }
}
