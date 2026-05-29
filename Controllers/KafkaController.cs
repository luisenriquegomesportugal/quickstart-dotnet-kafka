using Confluent.Kafka;
using Microsoft.AspNetCore.Mvc;

namespace dotnet_webapi.Controllers;

[ApiController]
[Route("[controller]")]
public class KafkaController : ControllerBase
{
    private readonly ILogger<KafkaController> _logger;

    private readonly string _bootstrapServers;
    private readonly string _topic;
    private readonly string _groupId;
    private readonly string _username;
    private readonly string _password;
    private readonly string _sslCaLocation;

    public KafkaController(ILogger<KafkaController> logger)
    {
        _logger = logger;

        _bootstrapServers =
            Environment.GetEnvironmentVariable("KAFKA_BOOTSTRAP_SERVERS")
            ?? throw new Exception("KAFKA_BOOTSTRAP_SERVERS not configured");

        _topic =
            Environment.GetEnvironmentVariable("KAFKA_TOPIC")
            ?? "teste-topic";

        _groupId =
            Environment.GetEnvironmentVariable("KAFKA_GROUP_ID")
            ?? "dotnet-group";

        _username =
            Environment.GetEnvironmentVariable("KAFKA_USERNAME")
            ?? throw new Exception("KAFKA_USERNAME not configured");

        _password =
            Environment.GetEnvironmentVariable("KAFKA_PASSWORD")
            ?? throw new Exception("KAFKA_PASSWORD not configured");

        _sslCaLocation =
            Environment.GetEnvironmentVariable("KAFKA_CA_LOCATION")
            ?? "/etc/kafka/certs/ca.crt";
    }

    [HttpGet]
    public async Task<IActionResult> Get()
    {
        _logger.LogInformation(
            "Iniciando teste Kafka em {DateTime}",
            DateTime.UtcNow);

        try
        {
            var message =
                $"Mensagem enviada em {DateTime.UtcNow:O}";

            var producerConfig = new ProducerConfig
            {
                BootstrapServers = _bootstrapServers,

                SecurityProtocol = SecurityProtocol.SaslSsl,

                SaslMechanism = SaslMechanism.ScramSha512,

                SaslUsername = _username,

                SaslPassword = _password,

                SslCaLocation = _sslCaLocation,

                SslEndpointIdentificationAlgorithm =
                    SslEndpointIdentificationAlgorithm.None
            };

            _logger.LogInformation(
                "Produzindo mensagem no tópico {Topic}",
                _topic);

            DeliveryResult<Null, string> produceResult;

            using (var producer =
                   new ProducerBuilder<Null, string>(producerConfig)
                   .Build())
            {
                produceResult = await producer.ProduceAsync(
                    _topic,
                    new Message<Null, string>
                    {
                        Value = message
                    });

                producer.Flush(TimeSpan.FromSeconds(10));
            }

            _logger.LogInformation(
                "Mensagem produzida com sucesso. Partition={Partition} Offset={Offset}",
                produceResult.Partition.Value,
                produceResult.Offset.Value);

            var consumerConfig = new ConsumerConfig
            {
                BootstrapServers = _bootstrapServers,

                GroupId = _groupId,

                AutoOffsetReset = AutoOffsetReset.Earliest,

                EnableAutoCommit = false,

                SecurityProtocol = SecurityProtocol.SaslSsl,

                SaslMechanism = SaslMechanism.ScramSha512,

                SaslUsername = _username,

                SaslPassword = _password,

                SslCaLocation = _sslCaLocation,

                SslEndpointIdentificationAlgorithm =
                    SslEndpointIdentificationAlgorithm.None
            };

            ConsumeResult<Ignore, string>? consumeResult;

            using (var consumer =
                   new ConsumerBuilder<Ignore, string>(consumerConfig)
                   .Build())
            {
                _logger.LogInformation(
                    "Consumindo mensagens do tópico {Topic}",
                    _topic);

                consumer.Subscribe(_topic);

                consumeResult =
                    consumer.Consume(TimeSpan.FromSeconds(10));

                consumer.Close();
            }

            if (consumeResult != null)
            {
                _logger.LogInformation(
                    "Mensagem consumida com sucesso. Offset={Offset}",
                    consumeResult.Offset.Value);
            }
            else
            {
                _logger.LogWarning(
                    "Nenhuma mensagem foi consumida");
            }

            return Ok(new
            {
                success = true,

                producer = new
                {
                    topic = produceResult.Topic,
                    partition = produceResult.Partition.Value,
                    offset = produceResult.Offset.Value,
                    value = message
                },

                consumer = consumeResult == null
                    ? null
                    : new
                    {
                        topic = consumeResult.Topic,
                        partition = consumeResult.Partition.Value,
                        offset = consumeResult.Offset.Value,
                        value = consumeResult.Message.Value
                    }
            });
        }
        catch (Exception ex)
        {
            _logger.LogError(
                ex,
                "Erro ao executar operação Kafka");

            return StatusCode(500, new
            {
                success = false,
                error = ex.Message
            });
        }
    }
}