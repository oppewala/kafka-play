using Confluent.Kafka;
using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry.Serdes;
using Kafka.Test.Overview.Data;
using Kafka.Test.Schemas;
using Schemas;

namespace Kafka.Test.Overview;

public class LatencyWorker : BackgroundService
{
    private readonly ILogger<LatencyWorker> _logger;
    private readonly LatencyService _latencyService;
    private IConsumer<string, Latency> _consumer;

    public LatencyWorker(ILogger<LatencyWorker> logger, LatencyService latencyService)
    {
        _logger = logger;
        _latencyService = latencyService;

        var configuration = KafkaConfig.AsDictionary();
        configuration["group.id"] = "Kafka.Test.Overview";
        configuration["auto.offset.reset"] = "earliest";
        
        _consumer = new ConsumerBuilder<string, Latency>(
                configuration.AsEnumerable())
            .SetValueDeserializer(new ProtobufDeserializer<Latency>().AsSyncOverAsync())
            .SetErrorHandler((_, e) => _logger.LogError("Error: ${Reason}", e.Reason))
            .Build();
    }
    
    protected override Task ExecuteAsync(CancellationToken stoppingToken)
    {
        return Task.Run(() => StartConsumerLoop(stoppingToken));
    }

    private async Task StartConsumerLoop(CancellationToken ct)
    {
        _consumer.Subscribe(KafkaConfig.Topics.LatencyMetrics);

        while (!ct.IsCancellationRequested)
        {
            try
            {
                var cr = _consumer.Consume(ct);

                var lr = new LatencyRecord(cr.Message.Timestamp.UnixTimestampMs, cr.Message.Value.Latency_);
                _latencyService.AddLatency(cr.Message.Value.Zone, cr.Message.Value.Application, lr);
                
                _logger.LogInformation("Processed record");
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
}