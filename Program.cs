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

        CancellationTokenSource cts = new CancellationTokenSource();
        Console.CancelKeyPress += (_, e) =>
        {
            e.Cancel = true;
            cts.Cancel();
        };

        using var consumer = new ConsumerBuilder<string, string>(consumerConfig).Build();
        using var producer = new ProducerBuilder<string, string>(producerConfig).Build();

        try
        {
            Console.WriteLine("[CONNECT] Conectando ao Kafka...");
            consumer.Subscribe(KAFKA_TOPIC);
            Console.WriteLine($"[SUBSCRIBE] Ouvindo o tópico: {KAFKA_TOPIC}");

            // 🟢 Mensagem inicial automática
            var startupMessage = $"[STARTUP] Consumidor iniciado às {DateTime.Now:HH:mm:ss}";
            Console.WriteLine($"[PRODUCE] Enviando mensagem inicial: {startupMessage}");

            var dr = await producer.ProduceAsync(KAFKA_TOPIC, new Message<string, string>
            {
                Key = "startup",
                Value = startupMessage
            }, cts.Token);

            Console.WriteLine($"[DELIVERED] Mensagem inicial publicada em {dr.TopicPartitionOffset}");

            // 🔁 Loop principal
            while (!cts.Token.IsCancellationRequested)
            {
                try
                {
                    var cr = consumer.Consume(cts.Token);

                    Console.WriteLine($"[MESSAGE RECEIVED] Offset: {cr.Offset}, Key: {cr.Message.Key ?? "(null)"}, Value: {cr.Message.Value}");

                    // Espera 5 segundos
                    Console.WriteLine("[WAIT] Aguardando 5 segundos antes de publicar resposta...");
                    await Task.Delay(5000, cts.Token);

                    var newMessage = $"(Echo) [{DateTime.Now:HH:mm:ss}] -> {cr.Message.Value}";
                    Console.WriteLine($"[PRODUCE] Enviando mensagem de resposta: {newMessage}");

                    var response = await producer.ProduceAsync(KAFKA_TOPIC, new Message<string, string>
                    {
                        Key = cr.Message.Key,
                        Value = newMessage
                    }, cts.Token);

                    Console.WriteLine($"[DELIVERED] Resposta publicada em {response.TopicPartitionOffset}");
                }
                catch (ConsumeException ex)
                {
                    Console.WriteLine($"[ERROR][CONSUME] {ex.Error.Reason}");
                }
                catch (ProduceException<string, string> ex)
                {
                    Console.WriteLine($"[ERROR][PRODUCE] Falha ao enviar mensagem: {ex.Error.Reason}");
                }
                catch (TaskCanceledException)
                {
                    Console.WriteLine("[CANCEL] Operação cancelada.");
                    break;
                }
            }
        }
        catch (OperationCanceledException)
        {
            Console.WriteLine("[STOP] Encerrando consumidor...");
        }
        finally
        {
            consumer.Close();
            Console.WriteLine("[CLOSE] Conexão encerrada.");
        }
    }
}
