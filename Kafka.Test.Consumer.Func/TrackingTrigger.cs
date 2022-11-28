// using System;
// using System.Diagnostics;
// using System.Text.Json;
// using Microsoft.Azure.Functions.Worker;
// using Microsoft.Extensions.Logging;
// using Schemas;
// using StackExchange.Redis;
//
// namespace Kafka.Test.Consumer.Func;
//
// public class TrackingTrigger
// {
//     private IServer _srv;
//     private readonly IDatabase _db;
//     
//     public TrackingTrigger(IConnectionMultiplexer redisConn)
//     {
//         _srv = redisConn.GetServers()[0];
//         _db = redisConn.GetDatabase();
//     }
//     
//     [Function("TrackingTrigger")]
//     public async Task Run([TimerTrigger("00:00:10.000")] MyInfo myTimer, FunctionContext context)
//     {
//         var logger = context.GetLogger("TimerTrigger");
//
//         await PrintValuesAsync("local", logger);
//         await PrintValuesAsync("azure", logger);
//
//         await PrintThroughput("local", logger);
//         await PrintThroughput("azure", logger);
//     }
//
//     private async Task<Dictionary<string, Stock>> GetValuesAsync(IAsyncEnumerable<RedisKey> keys)
//     {
//         Dictionary<string, Stock> dict = new Dictionary<string, Stock>();
//         await foreach (var key in keys)
//         {
//             var value = await _db.StringGetAsync(key);
//
//             dict[key.ToString()] = JsonSerializer.Deserialize<Stock>(value);
//         }
//
//         return dict;
//     }
//
//     private async Task PrintValuesAsync(string suffix, ILogger logger)
//     {
//         var keys = _srv.KeysAsync(pattern: $"stocks:{suffix}:*");
//         var stocks = await GetValuesAsync(keys);
//      
//         logger.LogInformation("{Suffix} stocks: {Values}", suffix, JsonSerializer.Serialize(stocks));
//     }
//
//     private async Task PrintThroughput(string suffix, ILogger logger)
//     {
//         var values = await _db.ListRangeAsync($"throughput:func:{suffix}");
//         var throughputs = values
//             .Select(t => JsonSerializer.Deserialize<ThroughputTracker>(t))
//             .ToList();
//
//         var lastMin = throughputs.Count(t => t.Timestamp >= DateTimeOffset.Now.AddMinutes(-1).ToUnixTimeMilliseconds());
//         var fiveMin =  throughputs.Count(t => t.Timestamp >= DateTimeOffset.Now.AddMinutes(-5).ToUnixTimeMilliseconds());
//         
//         logger.LogInformation("{Suffix} throughput - total: {Total}, 1min: {LastMin}, 5min: {FiveMin}", suffix, throughputs.Count, lastMin, fiveMin);
//         
//     }
// }
//
// public class MyInfo
// {
//     public MyScheduleStatus ScheduleStatus { get; set; }
//
//     public bool IsPastDue { get; set; }
// }
//
// public class MyScheduleStatus
// {
//     public DateTime Last { get; set; }
//
//     public DateTime Next { get; set; }
//
//     public DateTime LastUpdated { get; set; }
// }