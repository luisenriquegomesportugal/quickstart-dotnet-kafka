using Confluent.Kafka;
using Microsoft.Extensions.Configuration;

class Consumer
{

    static string KAFKA_SERVER = $"{Environment.GetEnvironmentVariable("KAFKA_SERVER")}";
    static string KAFKA_TOPIC = $"{Environment.GetEnvironmentVariable("KAFKA_TOPIC")}";
    static string KEYSTORE_LOCATION = $"{Environment.GetEnvironmentVariable("SSL_LOCATION")}";
    //static string KEYSTORE_LOCATION = "../src/Keystore/ca.p12";
    static string KEYSTORE_PASSWORD = $"{Environment.GetEnvironmentVariable("SSL_PASSWORD")}";
    static string SASL_CONFIG = $"{Environment.GetEnvironmentVariable("SASL_CONFIG")}";

    static void Main(string[] args)
    {
        if (args.Length != 1)
        {
            Console.WriteLine("Please provide the configuration file path as a command line argument");
        }

        var configuration = new Dictionary<string, string>
        {
            {"bootstrap-server", KAFKA_SERVER},
            {"security.protocol", "SASL_SSL"},
            {"ssl.truststore.location", KEYSTORE_LOCATION},
            {"ssl.truststore.password", KEYSTORE_PASSWORD},
            {"ssl.enabled.protocols", "TLSv1.2,TLSv1.1,TLSv1"},
            {"sasl.mechanism", "SCRAM-SHA-512"},
            {"sasl.jaas.config", SASL_CONFIG},
        };

        CancellationTokenSource cts = new CancellationTokenSource();
        Console.CancelKeyPress += (_, e) =>
        {
            e.Cancel = true; // prevent the process from terminating.
            cts.Cancel();
        };

        using (var consumer = new ConsumerBuilder<string, string>(configuration).Build())
        {
            consumer.Subscribe(KAFKA_TOPIC);
            try
            {
                while (true)
                {
                    var cr = consumer.Consume(cts.Token);
                    Console.WriteLine($"Consumed event from topic {KAFKA_TOPIC}: key = {cr.Message.Key,-10} value = {cr.Message.Value}");
                }
            }
            catch (OperationCanceledException)
            {
                // Ctrl-C was pressed.
            }
            finally
            {
                consumer.Close();
            }
        }
    }
}