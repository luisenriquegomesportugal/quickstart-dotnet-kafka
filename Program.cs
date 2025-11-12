using System;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;

class Consumer
{
    static string KAFKA_SERVER = $"{Environment.GetEnvironmentVariable("KAFKA_SERVER")}";
    static string KAFKA_GROUP = $"{Environment.GetEnvironmentVariable("KAFKA_GROUP")}";
    static string KAFKA_TOPIC = $"{Environment.GetEnvironmentVariable("KAFKA_TOPIC")}";
    static string KAFKA_CERT_LOCATION = $"{Environment.GetEnvironmentVariable("KAFKA_CERT_LOCATION")}";
    static string KAFKA_USERNAME = $"{Environment.GetEnvironmentVariable("KAFKA_USERNAME")}";
    static string KAFKA_PASSWORD = $"{Environment.GetEnvironmentVariable("KAFKA_PASSWORD")}";

    static async Task Main(string[] args)
    {
        Console.WriteLine("[INIT] Inicializando...");

        var consumerConfig = new ConsumerConfig
        {
            AutoOffsetReset = AutoOffsetReset.Earliest,
            BootstrapServers = KAFKA_SERVER,
            GroupId = KAFKA_GROUP,
            SecurityProtocol = SecurityProtocol.SaslSsl,
            SslCaLocation = KAFKA_CERT_LOCATION,
            SaslMechanism = SaslMechanism.ScramSha512,
            SaslUsername = KAFKA_USERNAME,
            SaslPassword = KAFKA_PASSWORD,
            EnableAutoCommit = true
        };

        var producerConfig = new ProducerConfig
        {
            BootstrapServers = KAFKA_SERVER,
            SecurityProtocol = SecurityProtocol.SaslSsl,
            SslCaLocation = KAFKA_CERT_LOCATION,
            SaslMechanism = SaslMechanism.ScramSha512,
            SaslUsername = KAFKA_USERNAME,
            SaslPassword = KAFKA_PASSWORD
        };

        using var consumer = new ConsumerBuilder<string, string>(consumerConfig).Build();
        using var producer = new ProducerBuilder<string, string>(producerConfig).Build();

        Console.WriteLine("[CONNECT] Conectando ao Kafka...");
        consumer.Subscribe(KAFKA_TOPIC);
        Console.WriteLine($"[SUBSCRIBE] Ouvindo o tópico: {KAFKA_TOPIC}");

        // 🕒 Aguarda 5 segundos antes de enviar mensagem
        Console.WriteLine("[WAIT] Aguardando 5 segundos antes de enviar mensagem...");
        await Task.Delay(5000);

        var message = $"[AUTO] Mensagem enviada automaticamente às {DateTime.UtcNow:O}";
        Console.WriteLine($"[PRODUCE] Enviando mensagem: {message}");

        try
        {
            var dr = await producer.ProduceAsync(KAFKA_TOPIC, new Message<string, string>
            {
                Key = "auto",
                Value = message
            });

            Console.WriteLine($"[DELIVERED] Mensagem publicada em {dr.TopicPartitionOffset}");
        }
        catch (ProduceException<string, string> ex)
        {
            Console.WriteLine($"[ERROR][PRODUCE] {ex.Error.Reason}");
        }

        Console.WriteLine("[LISTENING] Aguardando mensagens do tópico...");

        // 🔁 Loop contínuo de escuta
        try
        {
            var receivedMessage = false;
            while (!receivedMessage)
            {
                var cr = consumer.Consume();
                if (cr?.Message != null)
                {
                    Console.WriteLine($"[MESSAGE RECEIVED] Offset: {cr.Offset}, Key: {cr.Message.Key ?? "(null)"}, Value: {cr.Message.Value}");
                    receivedMessage = true;
                }
            }
        }
        catch (ConsumeException ex)
        {
            Console.WriteLine($"[ERROR][CONSUME] {ex.Error.Reason}");
        }
        catch (OperationCanceledException)
        {
            Console.WriteLine("[STOP] Execução cancelada pelo usuário.");
        }
        finally
        {
            consumer.Close();
            Console.WriteLine("[CLOSE] Conexão encerrada.");
        }
    }
}
