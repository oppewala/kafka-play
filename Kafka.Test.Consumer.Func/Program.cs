using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Kafka.Test.Schemas;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Schemas;
using StackExchange.Redis;

var host = new HostBuilder()
    .ConfigureFunctionsWorkerDefaults()
    .ConfigureServices(s =>
    {
        s.AddLogging();
        s.AddSingleton<CachedSchemaRegistryClient>(new CachedSchemaRegistryClient(new SchemaRegistryConfig()
        {
            Url = KafkaConfig.Registry.Url,
            // BasicAuthCredentialsSource = AuthCredentialsSource.UserInfo,
            BasicAuthUserInfo = $"{KafkaConfig.Registry.Username}:{KafkaConfig.Registry.Password}"
        }));
        s.AddSingleton<IProducer<string, Latency>>(sp =>
        {
            var srClient = sp.GetRequiredService<CachedSchemaRegistryClient>();
            var logger = sp.GetRequiredService<ILoggerFactory>().CreateLogger("ProducerBuilder");
            
            return new ProducerBuilder<string, Latency>(KafkaConfig.AsDictionary())
                .SetErrorHandler((_, e) => logger.LogError("Producer failed: {Reason}", e.Reason))
                .SetValueSerializer(new ProtobufSerializer<Latency>(srClient))
                .Build();
        });
        s.AddSingleton<IConnectionMultiplexer>(ConnectionMultiplexer.Connect("rd-kafka-test.redis.cache.windows.net:6380,password=EjApJGH0mo1ukLBgOdgpmy4ZNuugJ6MfyAzCaLrq3T8=,ssl=True,abortConnect=False"));
        s.AddSingleton<MaterializedView>();
        s.AddSingleton<KafkaStats>();
    })
    .Build();

host.Run();