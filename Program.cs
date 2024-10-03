using System.Diagnostics;
using LunaDB;

Console.WriteLine("starting lunadb...");

using var db = Database.Open("db");

var documentOffset = 0L;
var indexOffset = 0L;

var json = Enumerable.Range(0, 1024).Select(x => (byte)61).ToArray();
var watch = Stopwatch.StartNew();

for (var i = 1; i <= 1001; i++)
{
    indexOffset = db.WriteIndex(i, documentOffset, indexOffset);
    documentOffset = db.WriteDocument(i, json, documentOffset);
}

db.FlushToDisk();

Console.WriteLine($"disk flush took {watch.ElapsedMilliseconds}ms");

//watch.Restart();

//await foreach (var document in db.ScanAsync())
//{
    //    Console.WriteLine(
    //        $"id {document.Id}, data size is {document.Data.Length} bytes, data [{System.Text.Encoding.UTF8.GetString(document.Data.Span)}]"
    //    );
//}

//Console.WriteLine($"document scan took {watch.ElapsedMilliseconds}ms");

watch.Restart();

var doc = await db.FindByIdAsync(500);

Console.WriteLine(doc is null
    ? "failed to find document using index."
    : $"document find for id {doc.Id} took {watch.ElapsedMilliseconds}ms");

    Console.WriteLine(
        $"id {doc.Id}, data size is {doc.Data.Length} bytes, data [{System.Text.Encoding.UTF8.GetString(doc.Data.Span)}]"
    );