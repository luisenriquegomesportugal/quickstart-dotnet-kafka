using System.Security.Authentication;
using Confluent.Kafka;
using Microsoft.Extensions.Configuration;

class Consumer
{
    static void Main(string[] args)
    {
        Console.WriteLine("Initializing...");
        
        // var configuration = new Dictionary<string, string>();
        // configuration.Add("bootstrap.servers", "instance-kafka-bootstrap-kafka.apps.ocp.desenv.com:443");
        // configuration.Add("auto.offset.reset", "earliest");
        // configuration.Add("security.protocol", "SASL_SSL");
        // configuration.Add("group.id", "teste-nao-produtivo-group");
        
        // configuration.Add("ssl.key.location", "/opt/app-root/src/ca.crt");
        // configuration.Add("ssl.key.password", "HQL8lcZ18o4x");
        
        // configuration.Add("sasl.mechanism", "SCRAM-SHA-512");
        // configuration.Add("sasl.username", "sofintech-kafka");
        // configuration.Add("sasl.password", "F3Si8w05cCP6k8AQNtO9W67rDI2Te6uG");

        var configuration = new ConsumerConfig {
            AutoOffsetReset = AutoOffsetReset.Earliest,    
            BootstrapServers = "instance-kafka-bootstrap-kafka.apps.ocp.desenv.com:443",
            GroupId = "teste-nao-produtivo-group",
            SecurityProtocol = SecurityProtocol.SaslSsl,
            SslCaLocation = "/opt/app-root/src/ca.crt",  
            SaslMechanism = SaslMechanism.ScramSha512,
            SaslUsername = "sofintech-kafka",
            SaslPassword = "F3Si8w05cCP6k8AQNtO9W67rDI2Te6uG",      
        };

        CancellationTokenSource cts = new CancellationTokenSource();
        Console.CancelKeyPress += (_, e) =>
        {
            e.Cancel = true; // prevent the process from terminating.
            cts.Cancel();
        };

        using (var consumer = new ConsumerBuilder<string, string>(configuration).Build())
        {
            consumer.Subscribe("teste-nao-produtivo");
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
