using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Kafka.Test.Consumer.Api;
using Kafka.Test.Schemas;
using Schemas;

var builder = WebApplication.CreateBuilder(args);

builder.Logging.AddConsole();

// Add services to the container.

builder.Services.AddControllers();
// Learn more about configuring Swagger/OpenAPI at https://aka.ms/aspnetcore/swashbuckle
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

builder.Services.AddSingleton<CachedSchemaRegistryClient>(new CachedSchemaRegistryClient(new SchemaRegistryConfig()
{
    Url = KafkaConfig.Registry.Url,
    // BasicAuthCredentialsSource = AuthCredentialsSource.UserInfo,
    BasicAuthUserInfo = $"{KafkaConfig.Registry.Username}:{KafkaConfig.Registry.Password}"
}));
builder.Services.AddSingleton<IProducer<string, Latency>>(sp =>
{
    var srClient = sp.GetRequiredService<CachedSchemaRegistryClient>();
    var logger = sp.GetRequiredService<ILoggerFactory>().CreateLogger("ProducerBuilder");

    return new ProducerBuilder<string, Latency>(KafkaConfig.AsDictionary())
        .SetErrorHandler((_, e) => logger.LogError("Producer failed: {Reason}", e.Reason))
        .SetValueSerializer(new ProtobufSerializer<Latency>(srClient))
        .Build();
});

builder.Services.AddHostedService<Worker>();
// builder.Services.AddHostedService<LatencyWorker>();
builder.Services.AddSingleton<MaterializedView>();
// builder.Services.AddSingleton<KafkaStats>();

var app = builder.Build();

app.UseHttpLogging();

// Configure the HTTP request pipeline.
if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI();
}

app.UseHttpsRedirection();

app.UseAuthorization();

app.MapGet("/debug/mv", (MaterializedView mv) => mv.InnerDict())
    .WithName("MaterializedView")
    .WithOpenApi();

app.MapGet("/debug/stats", (KafkaStats ks) => ks)
    .WithName("Stats")
    .WithOpenApi();

app.Logger.LogInformation("App started");

app.Run();