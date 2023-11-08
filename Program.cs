using Confluent.Kafka;
using Microsoft.Extensions.Configuration;

class Consumer
{
    static void Main(string[] args)
    {
        Console.WriteLine("Initializing...");
        
        var configuration = new Dictionary<string, string>();
        configuration.Add("bootstrap.servers", "instance-kafka-bootstrap-kafka.apps.ocp.desenv.com:443");
        configuration.Add("security.protocol", "SASL_SSL");
        configuration.Add("ssl.truststore.location", "./ca.p12");
        configuration.Add("ssl.truststore.password", "fX95Ovo90ZEW");
        configuration.Add("ssl.enabled.protocols", "TLSv1.2,TLSv1.1,TLSv1");
        configuration.Add("sasl.mechanism", "SCRAM-SHA-512");
        configuration.Add("sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"sofintech-kafka\" password=\"F3Si8w05cCP6k8AQNtO9W67rDI2Te6uG\";");
        configuration.Add("group.id", "teste-nao-produtivo-group");
        configuration.Add("auto.offset.reset", "earliest");

        CancellationTokenSource cts = new CancellationTokenSource();
        Console.CancelKeyPress += (_, e) =>
        {
            e.Cancel = true; // prevent the process from terminating.
            cts.Cancel();
        };

        using (var consumer = new ConsumerBuilder<string, string>(configuration.AsEnumerable()).Build())
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
