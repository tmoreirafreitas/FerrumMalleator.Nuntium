using Confluent.Kafka;
using Confluent.Kafka.Admin;
using FerrumMalleator.Nuntium.Configuration;
using Microsoft.Extensions.Options;

namespace FerrumMalleator.Nuntium.Transport.Kafka
{
    internal sealed class KafkaAdminClient(IOptions<KafkaOptions> options) : IKafkaAdminClient
    {
        private readonly KafkaOptions _options = options.Value;

        public async Task CreateTopicsAsync(IEnumerable<TopicSpecification> topics)
        {
            using var admin = new AdminClientBuilder(new AdminClientConfig
            {
                BootstrapServers = _options.BootstrapServers
            }).Build();

            await admin.CreateTopicsAsync(topics);
        }
    }
}
