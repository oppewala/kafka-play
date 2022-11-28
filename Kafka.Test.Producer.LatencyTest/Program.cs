// See https://aka.ms/new-console-template for more information

using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Schemas;

var logger = LoggerFactory.Create(b => b.AddConsole()).CreateLogger("Console");
logger.LogInformation("Starting up process");

using var pb = new ProducerBuilder<string, long>(KafkaConfig.AsDictionary())
    .SetErrorHandler((_, e) => logger.LogError("Error: {Reason}", e.Reason))
    .Build();

logger.LogInformation("ProducerBuilder configured");

while (true)
{
    try
    {
        await pb.ProduceAsync("latency_test", new Message<string, long>()
        {
            Key = "latency",
            Value = DateTimeOffset.Now.ToUnixTimeMilliseconds()
        });
        logger.LogInformation("Message pumped");
        await Task.Delay(5000);
    }
    catch (OperationCanceledException)
    {
        logger.LogWarning("Application terminating");
        break;
    }
    catch (Exception ex)
    {
        logger.LogCritical(ex, "Produce failed");
        break;
    }
    finally
    {
        pb.Flush();
    }
}