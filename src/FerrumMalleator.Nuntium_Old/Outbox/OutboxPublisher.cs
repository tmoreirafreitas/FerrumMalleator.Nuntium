using FerrumMalleator.Nuntium.Abstractions.Persistence;
using FerrumMalleator.Nuntium.Abstractions.Publishers;
using FerrumMalleator.Nuntium.Builders;
using FerrumMalleator.Nuntium.Messaging.Envelopes;
using System;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace FerrumMalleator.Nuntium.Outbox
{
    public class OutboxPublisher : IMessagePublisher
    {
        private readonly IOutboxStore _outboxStore;
        private readonly MessageMetadataRegistry _messageTypeRegistry;

        public OutboxPublisher(IOutboxStore outboxStore, MessageMetadataRegistry messageTypeRegistry)
        {
            _outboxStore = outboxStore;
            _messageTypeRegistry = messageTypeRegistry;

        }
        public async Task PublishAsync<T>(T message, CancellationToken cancellationToken = default)
        {
            var messageType = _messageTypeRegistry.Get<T>();
            var envelope = new MessageEnvelope<T>(Guid.NewGuid(), messageType.Key, message);

            var outbox = new OutboxMessage
            {
                Id = envelope.MessageId,
                Type = envelope.MessageType,
                Payload = JsonSerializer.Serialize(envelope),
                OccurredOn = envelope.OccurredOn
            };

            await _outboxStore.AddAsync(outbox, cancellationToken);
        }
    }
}
