using Kafka.Test.Consumer.Worker;
using Kafka.Test.Consumer.Worker.Confluent;
using Schemas;

IHost host = Host.CreateDefaultBuilder(args)
    .ConfigureServices(services =>
    {
        services.AddSingleton<MaterializedView>();
        services.AddHostedService<Worker>();
    })
    .ConfigureLogging((ctx, builder) =>
    {
        builder.AddConsole();
    })
    .Build();

host.Run();