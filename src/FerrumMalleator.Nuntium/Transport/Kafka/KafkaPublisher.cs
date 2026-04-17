using Confluent.Kafka;
using FerrumMalleator.Nuntium.Abstractions;
using FerrumMalleator.Nuntium.Abstractions.Publishers;
using FerrumMalleator.Nuntium.Builders;
using FerrumMalleator.Nuntium.Configuration;
using FerrumMalleator.Nuntium.Messaging.Envelopes;
using Microsoft.Extensions.Options;
using System.Text.Json;

namespace FerrumMalleator.Nuntium.Transport.Kafka
{
    internal sealed class KafkaPublisher : IMessagePublisher, IMessageTransport, IDisposable
    {
        private readonly KafkaOptions _options;
        private readonly IProducer<string, string> _producer;
        private readonly ITopicResolver _topicResolver;
        private readonly MessageMetadataRegistry _messageTypeRegistry;
        private bool disposedValue;

        public KafkaPublisher(IOptions<KafkaOptions> options, ITopicResolver topicResolver, MessageMetadataRegistry messageTypeRegistry)
        {
            _options = options.Value;
            _topicResolver = topicResolver;
            _messageTypeRegistry = messageTypeRegistry;

            var producerConfig = new ProducerConfig
            {
                BootstrapServers = _options.BootstrapServers,
            };

            _options.ProducerConfigAction?.Invoke(producerConfig);

            try
            {
                _producer = new ProducerBuilder<string, string>(producerConfig).Build();
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException(
                """
                Failed to initialize Kafka producer.

                Possible causes:
                - Native libraries blocked by Windows Security (SmartScreen)
                - Missing OpenSSL dependencies
                - Platform mismatch

                Try:
                - Unblock DLLs (PowerShell: Unblock-File)
                - Run as administrator
                - Ensure x64 runtime
                """, ex);
            }
        }

        public async Task PublishAsync<T>(T message, CancellationToken cancellationToken = default)
        {
            var messageId = Guid.NewGuid();

            var topic = _topicResolver.Resolve<T>();
            var messageType = _messageTypeRegistry.Get<T>();
            var key = messageType.PartitionKey?.Invoke(message!) ?? messageId.ToString();

            var envelope = new MessageEnvelope<T>
            {
                MessageId = messageId,
                MessageType = messageType.Key,
                Payload = message!
            };

            var json = JsonSerializer.Serialize(envelope);

            var messageProduce = new Message<string, string>
            {
                Key = key,
                Value = json
            };

            try
            {
                await _producer.ProduceAsync(topic: topic, messageProduce, cancellationToken).ConfigureAwait(false);
            }
            catch (ProduceException<string, string>)
            {
                throw;
            }
            catch (Exception)
            {
                throw;
            }
        }

        public async Task SendAsync(string messageType, string payload, CancellationToken ct)
        {
            var metadata = _messageTypeRegistry.Get(messageType);
            var message = new Message<string, string>
            {
                Key = Guid.NewGuid().ToString(),
                Value = payload
            };

            try
            {
                await _producer.ProduceAsync(metadata.Topic, message, ct).ConfigureAwait(false);
            }
            catch (ProduceException<string, string>)
            {
                throw;
            }
            catch (Exception)
            {
                throw;
            }
        }

        private void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    _producer?.Flush(TimeSpan.FromSeconds(5));
                    _producer?.Dispose();
                }

                disposedValue = true;
            }
        }

        public void Dispose()
        {
            Dispose(disposing: true);
            GC.SuppressFinalize(this);
        }
    }
}
