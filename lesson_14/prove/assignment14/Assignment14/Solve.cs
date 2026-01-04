using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Threading.Tasks;
using Newtonsoft.Json.Linq;

namespace Assignment14;

public static class Solve
{
    private static readonly HttpClient HttpClient = new()
    {
        Timeout = TimeSpan.FromSeconds(180)
    };

    public const string TopApiUrl = "http://127.0.0.1:8123";

    public static async Task<JObject?> GetDataFromServerAsync(string url)
    {
        var raw = await HttpClient.GetStringAsync(url);
        if (string.IsNullOrWhiteSpace(raw)) return null;
        return JObject.Parse(raw);
    }
    private static ConcurrentDictionary<long, byte> _seenFamilies = new();
    private static ConcurrentDictionary<long, byte> _seenPeople = new();
    private static readonly object TreeLock = new();
    private static void ResetSeen()
    {
        _seenFamilies = new ConcurrentDictionary<long, byte>();
        _seenPeople = new ConcurrentDictionary<long, byte>();
    }
    private static async Task<Family?> FetchFamilyAsync(long familyId)
    {
        if (familyId <= 0) return null;
        var raw = await HttpClient.GetStringAsync($"{TopApiUrl}/family/{familyId}");
        return string.IsNullOrWhiteSpace(raw) ? null : Family.FromJson(raw);
    }
    private static async Task<Person?> FetchPersonAsync(long personId)
    {
        if (personId <= 0) return null;
        if (!_seenPeople.TryAdd(personId, 1)) return null;

        var raw = await HttpClient.GetStringAsync($"{TopApiUrl}/person/{personId}");
        return string.IsNullOrWhiteSpace(raw) ? null : Person.FromJson(raw);
    }
    private static void SafeAdd(Tree tree, Family fam)
    {
        lock (TreeLock) { tree.AddFamily(fam); }
    }
    private static void SafeAdd(Tree tree, Person p)
    {
        lock (TreeLock) { tree.AddPerson(p); }
    }

    // ===================================================================================================
    // Part 1 — Depth-first search 
    // ===================================================================================================
    public static async Task<bool> DepthFS(long familyId, Tree tree)
    {
        ResetSeen();

        async Task VisitFamilyAsync(long fid)
        {
            if (fid <= 0) return;
            if (!_seenFamilies.TryAdd(fid, 1)) return;

            var family = await FetchFamilyAsync(fid);
            if (family == null) return;
            SafeAdd(tree, family);

            var personTasks = new List<Task<Person?>>();
            if (family.HusbandId > 0) personTasks.Add(FetchPersonAsync(family.HusbandId));
            if (family.WifeId > 0) personTasks.Add(FetchPersonAsync(family.WifeId));
            if (family.Children != null)
            {
                foreach (var cid in family.Children)
                    if (cid > 0) personTasks.Add(FetchPersonAsync(cid));
            }

            var persons = await Task.WhenAll(personTasks);
            foreach (var p in persons) if (p != null) SafeAdd(tree, p);

            var husband = persons.FirstOrDefault(p => p != null && p.Id == family.HusbandId);
            var wife = persons.FirstOrDefault(p => p != null && p.Id == family.WifeId);

            var recTasks = new List<Task>(2);
            if (husband != null && husband.ParentId > 0) recTasks.Add(VisitFamilyAsync(husband.ParentId));
            if (wife != null && wife.ParentId > 0) recTasks.Add(VisitFamilyAsync(wife.ParentId));
            if (recTasks.Count > 0) await Task.WhenAll(recTasks);
        }

        await VisitFamilyAsync(familyId);
        return true;
    }

    // ===================================================================================================
    // Part 2 — Breadth-first search 
    // ===================================================================================================
    public static async Task<bool> BreathFS(long familyId, Tree tree)
    {
        ResetSeen();

        var currentLevel = new List<long>();
        if (familyId > 0) currentLevel.Add(familyId);

        while (currentLevel.Count > 0)
        {
            var nextLevelBag = new ConcurrentBag<long>();

            var tasks = currentLevel.Select(async fid =>
            {
                if (!_seenFamilies.TryAdd(fid, 1)) return;

                var family = await FetchFamilyAsync(fid);
                if (family == null) return;
                SafeAdd(tree, family);

                var personTasks = new List<Task<Person?>>();
                if (family.HusbandId > 0) personTasks.Add(FetchPersonAsync(family.HusbandId));
                if (family.WifeId > 0) personTasks.Add(FetchPersonAsync(family.WifeId));
                if (family.Children != null)
                {
                    foreach (var cid in family.Children)
                        if (cid > 0) personTasks.Add(FetchPersonAsync(cid));
                }
                var persons = await Task.WhenAll(personTasks);
                foreach (var p in persons) if (p != null) SafeAdd(tree, p);

                var husband = persons.FirstOrDefault(p => p != null && p.Id == family.HusbandId);
                var wife = persons.FirstOrDefault(p => p != null && p.Id == family.WifeId);

                if (husband != null && husband.ParentId > 0) nextLevelBag.Add(husband.ParentId);
                if (wife != null && wife.ParentId > 0) nextLevelBag.Add(wife.ParentId);
            }).ToList();

            await Task.WhenAll(tasks);

            currentLevel = nextLevelBag
                .Where(fid => fid > 0)
                .Distinct()
                .Where(fid => !_seenFamilies.ContainsKey(fid))
                .ToList();
        }

        return true;
    }
}