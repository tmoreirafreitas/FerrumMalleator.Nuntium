using Confluent.Kafka;
using FerrumMalleator.Nuntium.Abstractions;
using FerrumMalleator.Nuntium.Abstractions.Publishers;
using FerrumMalleator.Nuntium.Builders;
using FerrumMalleator.Nuntium.Configuration;
using FerrumMalleator.Nuntium.Diagnostics;
using FerrumMalleator.Nuntium.Messaging.Envelopes;
using Microsoft.Extensions.Options;
using System.Diagnostics;
using System.Text.Json;

namespace FerrumMalleator.Nuntium.Transport.Kafka
{
    internal sealed class KafkaPublisher : IMessagePublisher, IMessageTransport, IDisposable
    {
        private readonly IKafkaProducerFactory _factory;
        private readonly IProducer<string, string> _producer;
        private readonly ITopicResolver _topicResolver;
        private readonly MessageMetadataRegistry _messageTypeRegistry;
        private bool disposedValue;

        internal KafkaPublisher(IProducer<string, string> producer, ITopicResolver topicResolver, MessageMetadataRegistry messageTypeRegistry)
        {
            _producer = producer;
            _topicResolver = topicResolver;
            _messageTypeRegistry = messageTypeRegistry;
        }

        public KafkaPublisher(
            IOptions<KafkaOptions> options, 
            ITopicResolver topicResolver, 
            MessageMetadataRegistry messageTypeRegistry, 
            IKafkaProducerFactory factory)
        {
            _factory = factory;
            _topicResolver = topicResolver;
            _messageTypeRegistry = messageTypeRegistry;

            var producerConfig = new ProducerConfig
            {
                BootstrapServers = options.Value.BootstrapServers,
            };

            options.Value.ProducerConfigAction?.Invoke(producerConfig);

            try
            {
                _producer = _factory.Create(producerConfig);
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

        public async Task PublishAsync<T>(T message, CancellationToken ct = default)
        {
            using var activity = NuntiumDiagnostics.ActivitySource.StartActivity("nuntium.kafka.publish", ActivityKind.Producer);

            var start = Stopwatch.GetTimestamp();

            activity?.SetTag("messaging.system", "kafka");

            activity?.SetTag("messaging.operation", "publish");

            activity?.SetTag("messaging.message_type", typeof(T).Name);

            try
            {
                var messageId = Guid.NewGuid();

                var topic = _topicResolver.Resolve<T>();

                activity?.SetTag("messaging.destination.name", topic);

                var messageType = _messageTypeRegistry.Get<T>();

                var key = messageType.PartitionKey?.Invoke(message!) ?? messageId.ToString();

                activity?.SetTag("messaging.kafka.message_key", key);

                var envelope = new MessageEnvelope<T>
                {
                    MessageId = messageId,
                    MessageType = messageType.Key,
                    Payload = message!
                };

                activity?.SetTag("messaging.message_id", envelope.MessageId);

                var json = JsonSerializer.Serialize(envelope);

                var messageProduce = new Message<string, string>
                {
                    Key = key,
                    Value = json
                };

                activity?.AddEvent(new ActivityEvent("kafka.message.producing"));

                await _producer.ProduceAsync(topic, messageProduce, ct).ConfigureAwait(false);

                activity?.AddEvent(new ActivityEvent("kafka.message.published"));

                activity?.SetStatus(ActivityStatusCode.Ok);

                NuntiumDiagnostics.KafkaMessagesPublished.Add(1);

                NuntiumDiagnostics.MessagesPublished.Add(1);
            }
            catch (Exception ex)
            {
                activity?.SetStatus(ActivityStatusCode.Error);

                activity?.AddException(ex);

                NuntiumDiagnostics.KafkaPublishFailures.Add(1);

                NuntiumDiagnostics.MessagesFailed.Add(1);

                throw;
            }
            finally
            {
                var elapsed = Stopwatch.GetElapsedTime(start);

                NuntiumDiagnostics.KafkaPublishDuration.Record(elapsed.TotalMilliseconds);

                NuntiumDiagnostics.PublishDuration.Record(elapsed.TotalMilliseconds);
            }
        }

        public async Task SendAsync(string messageType, string payload, CancellationToken ct)
        {
            using var activity = NuntiumDiagnostics.ActivitySource.StartActivity("nuntium.kafka.transport.send", ActivityKind.Producer);

            var start = Stopwatch.GetTimestamp();

            activity?.SetTag("messaging.system", "kafka");

            activity?.SetTag("messaging.operation", "transport");

            activity?.SetTag("messaging.message_type", messageType);

            try
            {
                var metadata = _messageTypeRegistry.Get(messageType);

                activity?.SetTag("messaging.destination.name", metadata.Topic);

                var message = new Message<string, string>
                {
                    Key = Guid.NewGuid().ToString(),
                    Value = payload
                };

                activity?.AddEvent(new ActivityEvent("kafka.transport.producing"));

                await _producer
                    .ProduceAsync(metadata.Topic, message, ct)
                    .ConfigureAwait(false);

                activity?.AddEvent(new ActivityEvent("kafka.transport.sent"));

                activity?.SetStatus(ActivityStatusCode.Ok);

                NuntiumDiagnostics.MessagesTransported.Add(1);
            }
            catch (Exception ex)
            {
                activity?.SetStatus(ActivityStatusCode.Error);

                activity?.AddException(ex);

                NuntiumDiagnostics.KafkaPublishFailures.Add(1);

                NuntiumDiagnostics.MessagesFailed.Add(1);

                throw;
            }
            finally
            {
                var elapsed = Stopwatch.GetElapsedTime(start);

                NuntiumDiagnostics.TransportDuration.Record(elapsed.TotalMilliseconds);

                NuntiumDiagnostics.KafkaPublishDuration.Record(elapsed.TotalMilliseconds);
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