using System;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Threading.Tasks;
using Confluent.Kafka;
using OpenTelemetry;
using OpenTelemetry.Metrics;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;

class Consumer
{
    static string KAFKA_SERVER = Environment.GetEnvironmentVariable("KAFKA_SERVER");
    static string KAFKA_GROUP = Environment.GetEnvironmentVariable("KAFKA_GROUP");
    static string KAFKA_TOPIC = Environment.GetEnvironmentVariable("KAFKA_TOPIC");
    static string KAFKA_CERT_LOCATION = Environment.GetEnvironmentVariable("KAFKA_CERT_LOCATION");
    static string KAFKA_USERNAME = Environment.GetEnvironmentVariable("KAFKA_USERNAME");
    static string KAFKA_PASSWORD = Environment.GetEnvironmentVariable("KAFKA_PASSWORD");

    // 🔍 Tracing
    private static readonly ActivitySource ActivitySource = new("KafkaConsumerApp");

    // 📊 Métricas
    private static readonly Meter Meter = new("KafkaConsumerMetrics");
    private static readonly Counter<int> MessagesProduced = Meter.CreateCounter<int>("messages_produced");
    private static readonly Counter<int> MessagesConsumed = Meter.CreateCounter<int>("messages_consumed");

    static async Task Main(string[] args)
    {
        Console.WriteLine("[INIT] Inicializando...");

        // 🌐 Resource (identificação no observability stack)
        var resourceBuilder = ResourceBuilder.CreateDefault()
            .AddService("kafka-consumer-app");

        // 🔍 TRACING
        using var tracerProvider = Sdk.CreateTracerProviderBuilder()
            .SetResourceBuilder(resourceBuilder)
            .AddSource("KafkaConsumerApp")
            .AddConsoleExporter()
            .AddOtlpExporter() // envia para collector (Tempo, Jaeger, etc)
            .Build();

        // 📊 MÉTRICAS
        using var meterProvider = Sdk.CreateMeterProviderBuilder()
            .SetResourceBuilder(resourceBuilder)
            .AddMeter("KafkaConsumerMetrics")
            .AddRuntimeInstrumentation()
            .AddConsoleExporter()
            .AddOtlpExporter()
            .Build();

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

        await Task.Delay(2000);

        var message = $"[AUTO] Mensagem enviada automaticamente às {DateTime.UtcNow:O}";
        Console.WriteLine($"[PRODUCE] {message}");

        // 🔍 TRACE PRODUCER
        using (var activity = ActivitySource.StartActivity("Kafka Produce"))
        {
            activity?.SetTag("messaging.system", "kafka");
            activity?.SetTag("messaging.destination", KAFKA_TOPIC);

            try
            {
                var dr = await producer.ProduceAsync(KAFKA_TOPIC, new Message<string, string>
                {
                    Key = "auto",
                    Value = message
                });

                MessagesProduced.Add(1);

                Console.WriteLine($"[DELIVERED] {dr.TopicPartitionOffset}");
            }
            catch (ProduceException<string, string> ex)
            {
                activity?.SetStatus(ActivityStatusCode.Error, ex.Error.Reason);
                Console.WriteLine($"[ERROR][PRODUCE] {ex.Error.Reason}");
            }
        }

        // 🔁 CONSUMO
        try
        {
            var receivedMessage = false;

            while (!receivedMessage)
            {
                using var activity = ActivitySource.StartActivity("Kafka Consume");

                var cr = consumer.Consume();

                if (cr?.Message != null)
                {
                    MessagesConsumed.Add(1);

                    activity?.SetTag("messaging.system", "kafka");
                    activity?.SetTag("messaging.destination", KAFKA_TOPIC);
                    activity?.SetTag("messaging.kafka.offset", cr.Offset.Value);

                    Console.WriteLine($"[MESSAGE RECEIVED] Offset: {cr.Offset}, Key: {cr.Message.Key ?? "(null)"}, Value: {cr.Message.Value}");

                    receivedMessage = true;
                }
            }
        }
        catch (ConsumeException ex)
        {
            Console.WriteLine($"[ERROR][CONSUME] {ex.Error.Reason}");
        }

        Console.WriteLine("[END] Finalizado.");
    }
}