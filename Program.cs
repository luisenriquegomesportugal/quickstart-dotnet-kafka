using Confluent.Kafka;

class Consumer
{
    static string KAFKA_SERVER = $"{Environment.GetEnvironmentVariable("KAFKA_SERVER")}";
    static string KAFKA_GROUP = $"{Environment.GetEnvironmentVariable("KAFKA_GROUP")}";
    static string KAFKA_TOPIC = $"{Environment.GetEnvironmentVariable("KAFKA_TOPIC")}";
    static string KAFKA_CERT_LOCATION = $"{Environment.GetEnvironmentVariable("KAFKA_CERT_LOCATION")}";
    static string KAFKA_USERNAME = $"{Environment.GetEnvironmentVariable("KAFKA_USERNAME")}";
    static string KAFKA_PASSWORD = $"{Environment.GetEnvironmentVariable("KAFKA_PASSWORD")}";

    static void Main(string[] args)
    {
        Console.WriteLine("Initializing...");

        var configuration = new ConsumerConfig {
            AutoOffsetReset = AutoOffsetReset.Earliest,    
            BootstrapServers = KAFKA_SERVER,
            GroupId = KAFKA_GROUP,
            SecurityProtocol = SecurityProtocol.SaslSsl,
            SslCaLocation = KAFKA_CERT_LOCATION,  
            SaslMechanism = SaslMechanism.ScramSha512,
            SaslUsername = KAFKA_USERNAME,
            SaslPassword = KAFKA_PASSWORD,      
        };

        CancellationTokenSource cts = new CancellationTokenSource();
        Console.CancelKeyPress += (_, e) =>
        {
            e.Cancel = true; // prevent the process from terminating.
            cts.Cancel();
        };

        using (var consumer = new ConsumerBuilder<string, string>(configuration).Build())
        {    
            Console.WriteLine("Conectado...");

            consumer.Subscribe(KAFKA_TOPIC);
            Console.WriteLine("Ouvindo...");

            try
            {
                while (true)
                {
                    var cr = consumer.Consume(cts.Token);
                    Console.WriteLine($"Consumed event from topic: key = {cr.Message.Key,-10} value = {cr.Message.Value}");
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
