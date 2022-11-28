using Confluent.Kafka;
using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry.Serdes;
using Kafka.Test.Schemas;
using Schemas;

namespace Kafka.Test.Consumer.Api;

public class Worker : BackgroundService
{
    const string Topic = "topic_0";
    
    // List<Task<DeliveryResult<string, Latency>>> produceTasks = new();
    private Queue<long> _latencyResults = new();
    private readonly ILogger<Worker> _logger;
    private readonly MaterializedView _mv;
    private readonly IConfiguration _cfg;
    private readonly IProducer<string, Latency> _latencyProducer;
    private IConsumer<string, StockTrade>? _consumer;

    public Worker(ILogger<Worker> logger, MaterializedView mv, IConfiguration cfg, IProducer<string, Latency> latencyProducer)
    {
        _logger = logger;
        _mv = mv;
        _cfg = cfg;
        _latencyProducer = latencyProducer;

        var configuration = KafkaConfig.AsDictionary();
        configuration["group.id"] = cfg["KAFKA_CONSUMER_GROUP"] ?? "Kafka.Test.Consumer.Api.Default";
        configuration["auto.offset.reset"] = "earliest";

        _consumer = new ConsumerBuilder<string, StockTrade>(
                configuration.AsEnumerable())
            .SetValueDeserializer(new ProtobufDeserializer<StockTrade>().AsSyncOverAsync())
            .SetErrorHandler((_, e) => _logger.LogError("Error: {Reason}", e.Reason))
            .Build();
    }

    protected override Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("Task execution started");

        Task.Run<Task>(() => StartLatencyProducerLoop(stoppingToken));
        return Task.Run<Task>(() => StartConsumerLoop(stoppingToken));
    }

    private async Task StartLatencyProducerLoop(CancellationToken ct)
    {
        async Task ProduceLatencyResults(string zone, int limit)
        {
            var tasks = new List<Task<DeliveryResult<string, Latency>>>(limit);
            while (_latencyResults.TryDequeue(out var latency) && (limit == 0 || tasks.Count < limit))
            {
                tasks.Add(_latencyProducer.ProduceAsync(KafkaConfig.Topics.LatencyMetrics, new Message<string, Latency>()
                {
                    Key = $"api:{zone}",
                    Value = new Latency()
                    {
                        Zone = zone,
                        Application = "api",
                        Latency_ = latency
                    }
                }, ct));
            }

            await Task.WhenAll(tasks);
            
            if (tasks.Any())
                _logger.LogInformation("Produced {Count} events", tasks.Count);
        }
        
        var zone = _cfg["APP_ZONE"];
        while (!ct.IsCancellationRequested)
        {
            await ProduceLatencyResults(zone, 500);
        }
        
        _logger.LogInformation("Application terminating - {QueueCount} events left to publish", _latencyResults.Count);
        await ProduceLatencyResults(zone, 0);
    }

    private async Task StartConsumerLoop(CancellationToken ct)
    {
        _consumer.Subscribe(Topic);

        _logger.LogInformation("Subscription started");

        while (!ct.IsCancellationRequested)
        {
            try
            {
                var cr = _consumer.Consume(ct);

                var timestamp = cr.Message.Timestamp.UnixTimestampMs;
                var latency = DateTimeOffset.Now.ToUnixTimeMilliseconds() - timestamp;
                
                _mv.Set(cr.Message.Value.Symbol, new Stock(timestamp, cr.Message.Value.Price));
                
                _latencyResults.Enqueue(latency);

                _logger.LogInformation("Consumed event from topic {Topic} with key {Key} - {Timestamp:G} - {Value}",
                    Topic, cr.Message.Key, cr.Message.Timestamp.UtcDateTime, cr.Message.Value);
            }
            catch (OperationCanceledException)
            {
                // Ctrl-C was pressed.
                _logger.LogInformation("Operation cancelled - terminating");
                break;
            }
            catch (ConsumeException ex)
            {
                if (ex.Error.IsFatal)
                {
                    _logger.LogError(ex, "Consumer failed with FATAL");
                    break;
                }

                _logger.LogInformation(ex , "Consumer failed in handleable manner");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Consumer failed with unexpected error");
                break;
            }
        }
    }

    public override void Dispose()
    {
        _logger.LogInformation("Stopping worker service");
        
        _logger.LogInformation("{MV}", _mv.Loggify());
        
        _consumer?.Close();
        _consumer?.Dispose();
        
        base.Dispose();
    }
}