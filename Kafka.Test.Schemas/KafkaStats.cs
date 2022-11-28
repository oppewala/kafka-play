using System.Diagnostics;

namespace Schemas;

public class KafkaStats
{
    
    
    public long MessageConsumed { get; set; }
    
    public double UptimeSeconds => (DateTime.Now - Process.GetCurrentProcess().StartTime).TotalSeconds;

    public double RequestsPerSecond => MessageConsumed / UptimeSeconds;
}