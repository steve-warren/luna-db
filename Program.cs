using System.Diagnostics;
using LunaDB;

Console.WriteLine("starting lunadb...");

using var db = Database.Open("db");

var offset = 0L;

var watch = Stopwatch.StartNew();

for (var i = 1; i <= 100; i++)
{
    offset = db.WriteDocument(i, "abcdefgh"u8, offset);
}

db.FlushToDisk();

Console.WriteLine($"disk flush took {watch.ElapsedMilliseconds}ms");

watch.Restart();

foreach (var document in db.Scan())
{
    Console.WriteLine(
        $"id {document.Id} data {System.Text.Encoding.UTF8.GetString(document.Data.Span)}"
    );
}
Console.WriteLine($"document scan took {watch.ElapsedMilliseconds}ms");
