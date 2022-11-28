// using System.Text;
// using Confluent.Kafka;
// using Confluent.SchemaRegistry;
// using Confluent.SchemaRegistry.Serdes;
// using Google.Protobuf;
// using Kafka.Test.Schemas;
// using Microsoft.Azure.Functions.Worker;
// using Microsoft.Extensions.Configuration;
// using Microsoft.Extensions.Logging;
// using Newtonsoft.Json.Linq;
// using Schemas;
// using StackExchange.Redis;
//
// namespace Kafka.Test.Consumer.Func;
//
// public class LatencyTrigger
// {
//     private readonly IConfiguration _cfg;
//     private readonly IConnectionMultiplexer _redis;
//     private readonly CachedSchemaRegistryClient _schemaRegistryClient;
//     private const string ConsumerGroup = "Kafka.Test.Consumer.Func.Local.0";
//     
//     public LatencyTrigger(IConfiguration cfg, IConnectionMultiplexer redis, CachedSchemaRegistryClient schemaRegistryClient)
//     {
//         _cfg = cfg;
//         _redis = redis;
//         _schemaRegistryClient = schemaRegistryClient;
//     }
//     
//     [Function("LatencyTrigger")]
//     [KafkaOutput(
//         KafkaConfig.Broker,
//         KafkaConfig.Topics.LatencyMetrics,
//         Username = KafkaConfig.Username,
//         Password = KafkaConfig.Password,
//         AuthenticationMode = BrokerAuthenticationMode.Plain,
//         Protocol = BrokerProtocol.SaslSsl)]
//     public async Task<Latency> Run([KafkaTrigger(
//         KafkaConfig.Broker, 
//         KafkaConfig.Topics.LatencyTest,
//         Username = KafkaConfig.Username,
//         Password = KafkaConfig.Password,
//         AuthenticationMode = BrokerAuthenticationMode.Plain,
//         Protocol = BrokerProtocol.SaslSsl,
//         ConsumerGroup = ConsumerGroup,
//         IsBatched = true)] string[] eventData, 
//         FunctionContext context
//         )
//     {
//         var logger = context.GetLogger<LatencyTrigger>();
//         
//         var eventJobj = JObject.Parse(eventData[^1]);
//         //
//         // var body = Encoding.UTF8.GetBytes(eventJobj["Value"].ToString())[1..];
//         // var ts = Encoding.UTF8.GetString(body);
//
//         var timestamp = eventJobj["Timestamp"].ToObject<DateTimeOffset>();
//         var latency = DateTimeOffset.Now.ToUnixTimeMilliseconds() - timestamp.ToUnixTimeMilliseconds();
//
//         logger.LogInformation("Latency: {Latency}ms", latency);
//
//         var db = _redis.GetDatabase();
//
//         var suffix = _cfg["APP_ZONE"] ?? "azure";
//
//         await db.StreamAddAsync(new RedisKey($"latency:func:{suffix}"), new[]
//         {
//             new NameValueEntry(new RedisValue("latency"), new RedisValue(latency.ToString()))
//         });
//
//         // var serializer = new ProtobufSerializer<Latency>(_schemaRegistryClient);
//         //
//         // var bytes = await serializer.SerializeAsync(new Latency()
//         // {
//         //     Zone = suffix,
//         //     Application = "Func",
//         //     Latency_ = latency
//         // }, SerializationContext.Empty);
//
//         // return new FuncOutput()
//         // {
//         //     Kevent = Encoding.UTF8.GetString(bytes)
//         // };
//
//         return new Latency()
//         {
//             Zone = suffix,
//             Application = "Func",
//             Latency_ = latency
//         };
//     }
// }
//
// public class FuncOutput
// {
//     [KafkaOutput(
//         KafkaConfig.Broker,
//         KafkaConfig.Topics.LatencyMetrics,
//         Username = KafkaConfig.Username,
//         Password = KafkaConfig.Password,
//         AuthenticationMode = BrokerAuthenticationMode.Plain,
//         Protocol = BrokerProtocol.SaslSsl)]
//     public string Kevent { get; set; }
// }