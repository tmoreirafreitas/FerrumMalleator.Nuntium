using FerrumMalleator.Nuntium.Abstractions.Persistence;
using FerrumMalleator.Nuntium.Abstractions.Publishers;
using FerrumMalleator.Nuntium.Builders;
using FerrumMalleator.Nuntium.Messaging.Envelopes;
using System.Text.Json;

namespace FerrumMalleator.Nuntium.Outbox
{
    internal sealed class OutboxPublisher(IOutboxStore outboxStore, MessageMetadataRegistry messageTypeRegistry) : IMessagePublisher
    {
        private readonly IOutboxStore _outboxStore = outboxStore;
        private readonly MessageMetadataRegistry _messageTypeRegistry = messageTypeRegistry;

        public async Task PublishAsync<T>(T message, CancellationToken ct = default)
        {
            var messageType = _messageTypeRegistry.Get<T>();
            var envelope = new MessageEnvelope<T>
            {
                MessageId = Guid.NewGuid(),
                MessageType = messageType.Key,
                Payload = message!
            };

            var outbox = new OutboxMessage
            {
                Id = envelope.MessageId,
                Type = envelope.MessageType,
                Payload = JsonSerializer.Serialize(envelope),
                OccurredOn = envelope.OccurredOn
            };

            await _outboxStore.AddAsync(outbox, ct);
        }
    }
}
