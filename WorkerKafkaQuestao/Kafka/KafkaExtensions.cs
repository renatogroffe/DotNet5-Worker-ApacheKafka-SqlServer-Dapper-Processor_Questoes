using Microsoft.Extensions.Configuration;
using Confluent.Kafka;

namespace WorkerKafkaQuestao.Kafka
{
    public static class KafkaExtensions
    {
        public static IConsumer<Ignore, string> CreateConsumer(
            IConfiguration configuration)
        {
            return new ConsumerBuilder<Ignore, string>(
                new ConsumerConfig()
                {
                    BootstrapServers = configuration["ApacheKafka:Broker"],
                    SecurityProtocol = SecurityProtocol.SaslSsl,
                    SaslMechanism = SaslMechanism.Plain,
                    SaslUsername = configuration["ApacheKafka:Username"],
                    SaslPassword = configuration["ApacheKafka:Password"],
                    GroupId = configuration["ApacheKafka:GroupId"],
                    AutoOffsetReset = AutoOffsetReset.Earliest
                }).Build();
        }
    }
}