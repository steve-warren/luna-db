using LunaDB;

Console.WriteLine("starting lunadb...");

using var db = Database.OpenDatabase("db");

db.WriteDocument(1, "abcdefgh"u8, 0);
db.WriteDocument(2, "ijklmnop"u8, 14);
db.WriteDocument(3, "qrstuvwx"u8, 28);
db.WriteDocument(4, "yz"u8, 42);
