using System.Text;
using Confluent.Kafka;
using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry.Serdes;
using Google.Protobuf;
using Kafka.Test.Schemas;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Schemas;
using StackExchange.Redis;
using JsonSerializer = System.Text.Json.JsonSerializer;

namespace Kafka.Test.Consumer.Func;

public class KafkaTrigger
{
    const string Topic = "topic_0";
    
    private readonly IConfiguration _cfg;
    private readonly IConnectionMultiplexer _redisConn;
    private readonly IProducer<string, Latency> _latencyProducer;

    public KafkaTrigger(IConfiguration cfg, IConnectionMultiplexer redisConn, IProducer<string, Latency> latencyProducer)
    {
        _cfg = cfg;
        _redisConn = redisConn;
        _latencyProducer = latencyProducer;
    }

    [Function("KafkaTrigger")]
    public async Task Run([KafkaTrigger(
        KafkaConfig.Broker, 
        Topic,
        Username = KafkaConfig.Username,
        Password = KafkaConfig.Password,
        AuthenticationMode = BrokerAuthenticationMode.Plain,
        Protocol = BrokerProtocol.SaslSsl,
        ConsumerGroup = "%KAFKA_CONSUMER_GROUP%",
        IsBatched = true)] string[] eventDatas, 
        FunctionContext context,
        CancellationToken ct)
    {
        var logger = context.GetLogger<KafkaTrigger>();
        var zone = _cfg["APP_ZONE"] ?? "azure";

        List<Task> tasks = new List<Task>();
        foreach (var eventData in eventDatas)
        {
            var eventJobj = JObject.Parse(eventData);

            var timestamp = eventJobj["Timestamp"].ToObject<DateTimeOffset>();
            var latency = DateTimeOffset.Now.ToUnixTimeMilliseconds() - timestamp.ToUnixTimeMilliseconds();
        
            var key = eventJobj["Key"].ToString();
        
            // var body = eventJobj["Value"];
            // var ds = new ProtobufDeserializer<StockTrade>()
            //     .AsSyncOverAsync();
            // var trade = ds.Deserialize(Encoding.UTF8.GetBytes(body.ToString()), false, SerializationContext.Empty);
            // var stock = new Stock(timestamp.ToUnixTimeSeconds(), trade.Price);
            // var db = _redisConn.GetDatabase();
            // tasks.Add(db.StringSetAsync(new RedisKey($"stocks:{zone}:{key}"), new RedisValue(JsonSerializer.Serialize(stock))));

            tasks.Add(_latencyProducer.ProduceAsync(KafkaConfig.Topics.LatencyMetrics, new Message<string, Latency>()
            {
                Key = $"func:{zone}",
                Value = new Latency()
                {
                    Zone = zone,
                    Application = "func",
                    Latency_ = latency
                }
            }, ct));

            logger.LogInformation("Consumed event from topic {Topic} with key {Key} - {Timestamp:G}",
                Topic, key, timestamp);
        }

        await Task.WhenAll(tasks);
    }
}