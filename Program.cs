using System.Diagnostics;
using LunaDB;

Console.WriteLine("starting lunadb...");

using var db = Database.Open("db");

var offset = 0L;
var json = Enumerable.Range(0, 1024).Select(x => (byte) 61).ToArray();
var watch = Stopwatch.StartNew();

for (var i = 1; i <= 1001; i++)
{
    offset = db.WriteDocument(i, json, offset);
}

db.FlushToDisk();

Console.WriteLine($"disk flush took {watch.ElapsedMilliseconds}ms");

watch.Restart();

await foreach (var document in db.ScanAsync())
{
//    Console.WriteLine(
//        $"id {document.Id}, data size is {document.Data.Length} bytes, data [{System.Text.Encoding.UTF8.GetString(document.Data.Span)}]"
//    );
}
Console.WriteLine($"document scan took {watch.ElapsedMilliseconds}ms");
