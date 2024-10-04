using System.Diagnostics;
using System.Text;
using LunaDB;

Console.WriteLine("starting lunadb...");

using var db = Database.Open("db");

var documentOffset = 0L;
var indexOffset = 0L;

var json = Encoding.UTF8.GetBytes("""
                          {
                            "id": 1,
                            "first_name": "Libbey",
                            "last_name": "Claessens",
                            "email": "lclaessens0@nyu.edu",
                            "ip_address": "187.249.82.137"
                          }
                          """).AsMemory();
var watch = Stopwatch.StartNew();

for (var i = 1; i <= 1; i++)
{
    indexOffset = db.WriteIndex(i, documentOffset, indexOffset);
    documentOffset = db.WriteDocument(i, json.Span, documentOffset);
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

var doc = await db.FindByIdAsync(1);

Console.WriteLine(doc is null
    ? "failed to find document using index."
    : $"document find for id {doc.Id} took {watch.ElapsedMilliseconds}ms");

if (doc is not null)
    Console.WriteLine(
        $"id {doc.Id}, data size is {doc.Data.Length} bytes, data [{System.Text.Encoding.UTF8.GetString(doc.Data.Span)}]"
    );

await db.TombstoneAsync(1);

db.FlushToDisk();

Console.WriteLine("deleted document.");

doc = await db.FindByIdAsync(1);

Console.WriteLine("deleted document " + (doc is not null ? "found" : "not found"));
