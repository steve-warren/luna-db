using LunaDB.Buffers;

Console.WriteLine("starting lunadb...");

using var file = File.OpenHandle(
    path: "db",
    mode: FileMode.Create,
    access: FileAccess.Write,
    share: FileShare.None,
    options: FileOptions.None
);

// initial file layout
// |-------------|----------------------|-----------------------|
// | id (int 32) | data length (int 16) | data (max 1 Kibibyte) |
// |-------------|----------------------|-----------------------|

const int OneKibibyte = 1024;

var buffer = new byte[sizeof(int) + sizeof(short) + OneKibibyte].AsSpan();

BinaryPrimitives.WriteInt32(buffer[0..4], 1);
BinaryPrimitives.WriteInt16(buffer[4..6], 8);
"abcdefgh"u8.CopyTo(buffer[6..]);

RandomAccess.Write(file, buffer[0..14], 0);
