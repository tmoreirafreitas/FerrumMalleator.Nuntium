using Confluent.Kafka;
using Confluent.Kafka.Admin;
using FerrumMalleator.Nuntium.Builders;
using FerrumMalleator.Nuntium.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;

namespace FerrumMalleator.Nuntium.Transport.Kafka
{
    internal sealed class KafkaTopicProvisioner(IKafkaAdminClient admin, IOptions<KafkaOptions> options, MessageMetadataRegistry registry) : BackgroundService
    {
        private readonly KafkaOptions _options = options.Value;
        private readonly MessageMetadataRegistry _registry = registry;
        private readonly IKafkaAdminClient _admin = admin;

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            await ProvisionAsync(stoppingToken);
        }
        internal async Task ProvisionAsync(CancellationToken ct)
        {
            var topics = _registry.GetAll()
                .Select(x => new TopicSpecification
                {
                    Name = x.Topic,
                    NumPartitions = _options.DefaultNumPartitions,
                    ReplicationFactor = _options.DefaultReplicationFactor
                })
                .ToList();

            if (topics.Count == 0)
                return;

            try
            {
                await _admin.CreateTopicsAsync(topics);
            }
            catch (CreateTopicsException ex)
            {
                foreach (var result in ex.Results)
                {
                    if (result.Error.Code != ErrorCode.TopicAlreadyExists)
                        throw;
                }
            }
        }
    }
}