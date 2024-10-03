namespace LunaDB;

public record Document(int Id, ReadOnlyMemory<byte> Data);