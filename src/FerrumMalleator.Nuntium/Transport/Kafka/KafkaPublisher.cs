using FerrumMalleator.Nuntium.Abstractions;
using FerrumMalleator.Nuntium.Abstractions.Publishers;
using FerrumMalleator.Nuntium.Builders;
using FerrumMalleator.Nuntium.Diagnostics;
using FerrumMalleator.Nuntium.Messaging.Envelopes;
using System.Diagnostics;
using System.Text.Json;

namespace FerrumMalleator.Nuntium.Transport.Kafka
{
    internal sealed class KafkaPublisher(
        IMessageTransport transport, 
        ITopicResolver topicResolver, 
        MessageMetadataRegistry messageTypeRegistry) : IMessagePublisher
    {
        private readonly IMessageTransport _transport = transport;

        private readonly ITopicResolver _topicResolver = topicResolver;

        private readonly MessageMetadataRegistry _messageTypeRegistry = messageTypeRegistry;

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

                var envelope = new MessageEnvelope<T>
                {
                    MessageId = messageId,
                    MessageType = messageType.Key,
                    Payload = message!
                };

                activity?.SetTag("messaging.message_id", envelope.MessageId);

                var json = JsonSerializer.Serialize(envelope);

                activity?.AddEvent(new ActivityEvent("message.serialized"));

                await _transport.SendAsync(topic, json, ct);

                activity?.AddEvent(new ActivityEvent("message.sent"));

                activity?.SetStatus(ActivityStatusCode.Ok);

                NuntiumDiagnostics.MessagesPublished.Add(1);
            }
            catch (Exception ex)
            {
                activity?.SetStatus(ActivityStatusCode.Error);

                activity?.AddException(ex);

                NuntiumDiagnostics.MessagesFailed.Add(1);

                throw;
            }
            finally
            {
                var elapsed = Stopwatch.GetElapsedTime(start);

                NuntiumDiagnostics.PublishDuration.Record(elapsed.TotalMilliseconds);
            }
        }
    }
}