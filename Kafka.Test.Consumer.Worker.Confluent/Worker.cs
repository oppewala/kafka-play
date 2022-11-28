using Confluent.Kafka;
using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry.Serdes;
using Kafka.Test.Schemas;
using Schemas;

namespace Kafka.Test.Consumer.Worker.Confluent;

public class Worker : BackgroundService
{
    const string Topic = "topic_0";
    
    private readonly ILogger<Worker> _logger;
    private readonly IConfiguration _cfg;
    private readonly MaterializedView _mv;
    private IConsumer<string, StockTrade> _consumer;

    public Worker(ILogger<Worker> logger, IConfiguration cfg, MaterializedView mv)
    {
        _logger = logger;
        _cfg = cfg;
        _mv = mv;
        
        
        var configuration = KafkaConfig.AsDictionary();
        configuration["group.id"] = cfg["KAFKA_CONSUMER_GROUP"] ?? "Kafka.Test.Consumer.Worker.Default";
        configuration["auto.offset.reset"] = "earliest";
        
        _consumer = new ConsumerBuilder<string, StockTrade>(
                configuration.AsEnumerable())
            .SetValueDeserializer(new ProtobufDeserializer<StockTrade>().AsSyncOverAsync())
            .SetErrorHandler((_, e) => _logger.LogError("Error: ${Reason}", e.Reason))
            .Build();
    }

    protected override Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("Task execution started");

        return Task.Run(() => StartConsumerLoop((stoppingToken)));
    }

    private void StartConsumerLoop(CancellationToken ct)
    {
        _consumer.Subscribe(Topic);
        
        _logger.LogInformation("Subscription started");

        while (!ct.IsCancellationRequested)
        {

            try
            {
                var cr = _consumer.Consume(ct);
                _mv.Set(cr.Message.Key, new Stock(cr.Message.Timestamp.UnixTimestampMs, cr.Message.Value.Price));

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