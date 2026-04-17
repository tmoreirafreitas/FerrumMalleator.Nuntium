using Confluent.Kafka;
using FerrumMalleator.Nuntium.Abstractions;
using FerrumMalleator.Nuntium.Abstractions.Publishers;
using FerrumMalleator.Nuntium.Builders;
using FerrumMalleator.Nuntium.Configuration;
using FerrumMalleator.Nuntium.Messaging.Envelopes;
using Microsoft.Extensions.Options;
using System;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace FerrumMalleator.Nuntium.Transport.Kafka
{
    public class KafkaPublisher : IMessagePublisher
    {
        private readonly IProducer<string, string> _producer;
        private readonly ITopicResolver _topicResolver;
        private readonly MessageMetadataRegistry _messageTypeRegistry;

        public KafkaPublisher(IOptions<KafkaOptions> options, ITopicResolver topicResolver, MessageMetadataRegistry messageTypeRegistry)
        {
            _topicResolver = topicResolver;
            _messageTypeRegistry = messageTypeRegistry;

            var config = options.Value;

            var producerConfig = new ProducerConfig
            {
                BootstrapServers = config.BootstrapServers,
                SecurityProtocol = config.SecurityProtocol,
                SaslUsername = config.SaslUsername,
                SaslPassword = config.SaslPassword,
            };

            _producer = new ProducerBuilder<string, string>(producerConfig).Build();
        }

        public async Task PublishAsync<T>(T message, CancellationToken cancellationToken = default)
        {
            var topic = _topicResolver.Resolve<T>();
            var messageType = _messageTypeRegistry.Get<T>();

            var envelope = new MessageEnvelope<T>(Guid.NewGuid(), messageType.Key, message);            

            var json = JsonSerializer.Serialize(envelope);

            await _producer.ProduceAsync(
                topic: topic,
                new Message<string, string>
                {
                    Key = envelope.MessageId.ToString(),
                    Value = json
                },
                cancellationToken);
        }
    }
}
