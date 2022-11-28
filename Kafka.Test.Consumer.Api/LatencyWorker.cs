// using Confluent.Kafka;
// using Confluent.Kafka.SyncOverAsync;
// using Confluent.SchemaRegistry.Serdes;
// using Kafka.Test.Schemas;
// using Schemas;
//
// namespace Kafka.Test.Consumer.Api;
//
// public class LatencyWorker : BackgroundService
// {
//     const string Topic = "latency_test";
//     
//     private readonly ILogger<LatencyWorker> _logger;
//     private readonly IConfiguration _cfg;
//     private readonly MaterializedView _mv;
//     private readonly KafkaStats _kafkaStats;
//     private IConsumer<string, long>? _consumer;
//
//     public LatencyWorker(ILogger<LatencyWorker> logger, IConfiguration cfg, MaterializedView mv, KafkaStats kafkaStats)
//     {
//         _logger = logger;
//         _cfg = cfg;
//         _mv = mv;
//         _kafkaStats = kafkaStats;
//
//         var configuration = KafkaConfig.AsDictionary();
//         configuration["group.id"] = cfg["KAFKA_CONSUMER_GROUP"] ?? "Kafka.Test.Consumer.Api.Default";
//         configuration["auto.offset.reset"] = "earliest";
//
//         _consumer = new ConsumerBuilder<string, long>(
//                 configuration.AsEnumerable())
//             .SetErrorHandler((_, e) => _logger.LogError("Error: {Reason}", e.Reason))
//             .Build();
//     }
//
//     protected override Task ExecuteAsync(CancellationToken stoppingToken)
//     {
//         _logger.LogInformation("Task execution started");
//
//         return Task.Run(() => StartConsumerLoop((stoppingToken)), stoppingToken);
//     }
//
//     private void StartConsumerLoop(CancellationToken ct)
//     {
//         _consumer.Subscribe(Topic);
//
//         _logger.LogInformation("Subscription started");
//
//         while (!ct.IsCancellationRequested)
//         {
//
//             try
//             {
//                 var cr = _consumer.Consume(ct);
//
//                 var latency = DateTimeOffset.Now.ToUnixTimeMilliseconds() - cr.Message.Value;
//
//                 _logger.LogInformation("Latency: {Latency}ms", latency);
//             }
//             catch (OperationCanceledException)
//             {
//                 // Ctrl-C was pressed.
//                 _logger.LogInformation("Operation cancelled - terminating");
//                 break;
//             }
//             catch (ConsumeException ex)
//             {
//                 if (ex.Error.IsFatal)
//                 {
//                     _logger.LogError(ex, "Consumer failed with FATAL");
//                     break;
//                 }
//
//                 _logger.LogInformation(ex , "Consumer failed in handleable manner");
//             }
//             catch (Exception ex)
//             {
//                 _logger.LogError(ex, "Consumer failed with unexpected error");
//                 break;
//             }
//         }
//     }
//
//     public override void Dispose()
//     {
//         _consumer?.Close();
//         _consumer?.Dispose();
//         
//         base.Dispose();
//     }
// }