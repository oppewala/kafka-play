namespace Schemas;

public static class KafkaConfig
{
    public const string Broker = "<kafka-endpoint>";
    public const string Username = "<kafka-username>";
    public const string Password = "<kafka-password>";

    public static Dictionary<string, string> AsDictionary() =>
        new Dictionary<string, string>(new[]
        {
            new KeyValuePair<string, string>("bootstrap.servers", Broker),
            new KeyValuePair<string, string>("security.protocol", "SASL_SSL"),
            new KeyValuePair<string, string>("sasl.mechanisms", "PLAIN"),
            new KeyValuePair<string, string>("sasl.username", Username),
            new KeyValuePair<string, string>("sasl.password", Password),
        });

    public class Topics
    {
        public const string LatencyTest = "latency_test";
        public const string LatencyMetrics = "metrics-latency";
    }

    public class Registry
    {
        public const string Url = "<registry-endpoint>";
        public const string Username = "<registry-username>";
        public const string Password = "<registry-password>";
    }
}