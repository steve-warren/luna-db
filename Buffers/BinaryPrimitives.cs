namespace LunaDB.Buffers;

using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

public static class BinaryPrimitives
{
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void EnsureLittleEndianness()
    {
        if (BitConverter.IsLittleEndian is false)
            throw new NotSupportedException("Big Endian byte order not supported.");
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void WriteInt16(Span<byte> destination, short value) =>
        MemoryMarshal.Write<short>(destination, in value);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void WriteInt32(Span<byte> destination, int value) =>
        MemoryMarshal.Write<int>(destination, in value);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void WriteInt64(Span<byte> destionation, long value) =>
        MemoryMarshal.Write<long>(destionation, in value);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static short ReadInt16(Span<byte> source) => MemoryMarshal.Read<short>(source);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static int ReadInt32(Span<byte> source) => MemoryMarshal.Read<int>(source);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static long ReadInt64(Span<byte> source) => MemoryMarshal.Read<long>(source);
}
