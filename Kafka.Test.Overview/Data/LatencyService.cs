using System.Collections.Concurrent;
using Kafka.Test.Schemas;
using Microsoft.AspNetCore.Components.Web;

namespace Kafka.Test.Overview.Data;

public class LatencyService
{
    public ConcurrentDictionary<string, LatencyBag> Latencies { get; } = new ();

    private bool WhereTimerange(LatencyRecord lr, int min)
    {
        return lr.Timestamp > DateTimeOffset.Now.AddMinutes(min).ToUnixTimeMilliseconds();
    }

    public void AddLatency(string zone, string application, LatencyRecord latency)
    {
        string key = $"{application}:{zone}";
        if (!Latencies.TryGetValue(key, out var bag))
        {
            bag = new LatencyBag();
            if (!Latencies.TryAdd(key, bag))
            {
                bag = Latencies[key];
            }
        }
        
        bag.Add(latency);
    }
}

public class LatencyBag
{
    public ConcurrentBag<LatencyRecord> Latencies = new();

    private DateTime Cached1MinExpiry = DateTime.Now.AddMinutes(-1);
    private object Cached1minLock = new object();
    private List<LatencyRecord> Cached1MinInner = new();

    public List<LatencyRecord> Cached1Min
    {
        get
        {
            if (Cached1MinExpiry > DateTime.Now)
            {
                lock (Cached1minLock)
                {
                    if (Cached1MinExpiry <= DateTime.Now)
                        return Cached1MinInner;

                    Cached1MinInner.Clear();
                    Cached1MinInner.AddRange(Latencies
                        .Where(lr => lr.Timestamp > DateTimeOffset.Now.AddMinutes(-1).ToUnixTimeMilliseconds()));
                    Cached1MinExpiry = DateTime.Now.AddMinutes(1);
                }
            }
            
            return Cached1MinInner;
        }
    }
    
    private DateTime Cached5MinExpiry = DateTime.Now.AddMinutes(-1);
    private object Cached5MinLock = new object();
    private List<LatencyRecord> Cached5MinInner = new();

    public List<LatencyRecord> Cached5Min
    {
        get
        {
            if (Cached5MinExpiry > DateTime.Now)
            {
                lock (Cached5MinLock)
                {
                    if (Cached5MinExpiry <= DateTime.Now)
                        return Cached1MinInner;

                    Cached5MinInner.Clear();
                    Cached5MinInner.AddRange(Latencies
                        .Where(lr => lr.Timestamp > DateTimeOffset.Now.AddMinutes(-1).ToUnixTimeMilliseconds()));
                    Cached5MinExpiry = DateTime.Now.AddMinutes(1);
                }
            }
            
            return Cached5MinInner;
        }
    }
    
    public void Add(LatencyRecord record)
    {
        Latencies.Add(record);

        Count++;
        Total += record.Latency;
        
        if (Min == null || Min > record.Latency) Min = record.Latency;
        if (Max == null || Max < record.Latency) Max = record.Latency;
    }

    public long? Min;
    public long? Max;
    public long Count = 0;
    public long Total = 0;
    public long Avg => Count > 0 ? Total / Count : 0;
    
    private bool WhereTimerange(LatencyRecord lr, int min)
    {
        return lr.Timestamp > DateTimeOffset.Now.AddMinutes(min).ToUnixTimeMilliseconds();
    }
    
    public long Min1min => Cached1Min
        .Min(lr => lr.Latency);
    
    public long Min5min => Cached5Min
        .Min(lr => lr.Latency);
    
    public long Max1min => Cached1Min
        .Max(lr => lr.Latency);
    
    public long Max5min => Cached5Min
        .Max(lr => lr.Latency);
    
    public double Avg1min => Cached1Min
        .Average(lr => lr.Latency);
    
    public double Avg5min => Cached5Min
        .Average(lr => lr.Latency);
}

public record LatencyRecord(long Timestamp, long Latency);