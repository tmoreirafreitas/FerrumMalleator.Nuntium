using FerrumMalleator.Nuntium.Abstractions.Persistence;
using FerrumMalleator.Nuntium.Abstractions.Publishers;
using FerrumMalleator.Nuntium.Builders;
using FerrumMalleator.Nuntium.Diagnostics;
using FerrumMalleator.Nuntium.Messaging.Envelopes;
using System.Diagnostics;
using System.Text.Json;

namespace FerrumMalleator.Nuntium.Outbox
{
    internal sealed class OutboxPublisher(IOutboxStore outboxStore, MessageMetadataRegistry messageTypeRegistry) : IMessagePublisher
    {
        private readonly IOutboxStore _outboxStore = outboxStore;

        private readonly MessageMetadataRegistry _messageTypeRegistry = messageTypeRegistry;

        public async Task PublishAsync<T>(T message, CancellationToken ct = default)
        {
            var start = Stopwatch.GetTimestamp();

            using var activity = NuntiumDiagnostics.ActivitySource.StartActivity("nuntium.outbox.publish", ActivityKind.Producer);

            activity?.SetTag("messaging.system", "nuntium");

            activity?.SetTag("messaging.operation", "publish");

            activity?.SetTag("messaging.destination.kind", "outbox");

            try
            {
                var messageType = _messageTypeRegistry.Get<T>();

                activity?.SetTag("messaging.message_type", messageType.Key);

                var envelope = new MessageEnvelope<T>
                {
                    MessageId = Guid.NewGuid(),
                    MessageType = messageType.Key,
                    Payload = message!
                };

                activity?.SetTag("messaging.message_id", envelope.MessageId);

                var outbox = new OutboxMessage
                {
                    Id = envelope.MessageId,
                    Type = envelope.MessageType,
                    Payload = JsonSerializer.Serialize(envelope),
                    OccurredOn = envelope.OccurredOn
                };

                activity?.AddEvent(new ActivityEvent("outbox.message.created"));

                await _outboxStore.AddAsync(outbox, ct);

                activity?.AddEvent(new ActivityEvent("outbox.message.stored"));

                activity?.SetStatus(ActivityStatusCode.Ok);

                NuntiumDiagnostics.MessagesPublished.Add(1);

                NuntiumDiagnostics.OutboxMessagesStored.Add(1);
            }
            catch (Exception ex)
            {
                activity?.SetStatus(ActivityStatusCode.Error);

                activity?.AddException(ex);

                NuntiumDiagnostics.OutboxFailures.Add(1);

                NuntiumDiagnostics.MessagesFailed.Add(1);

                throw;
            }
            finally
            {
                var elapsed = Stopwatch.GetElapsedTime(start);

                NuntiumDiagnostics.PublishDuration.Record(elapsed.TotalMilliseconds);

                NuntiumDiagnostics.OutboxStoreDuration.Record(elapsed.TotalMilliseconds);
            }
        }
    }
}