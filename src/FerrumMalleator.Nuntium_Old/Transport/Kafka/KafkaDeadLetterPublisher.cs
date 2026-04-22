using FerrumMalleator.Nuntium.Abstractions.Publishers;
using FerrumMalleator.Nuntium.Messaging.Envelopes;
using FerrumMalleator.Nuntium.Messaging.Models;
using System;
using System.Text.Json;
using System.Threading.Tasks;

namespace FerrumMalleator.Nuntium.Transport.Kafka
{
    internal sealed class KafkaDeadLetterPublisher : IDeadLetterPublisher
    {
        private readonly IMessagePublisher _publisher;
        public KafkaDeadLetterPublisher(IMessagePublisher publisher)
        {
            _publisher = publisher;
        }

        public async Task PublishAsync(IMessageEnvelope envelope, Exception ex, int retryCount = 0)
        {
            var dlq = new DeadLetterMessage(
                envelope.MessageId,
                envelope.MessageType,
                JsonSerializer.Serialize(envelope.Payload),
                ex.Message,
                ex.StackTrace,
                retryCount,
                envelope.OccurredOn,
                DateTime.UtcNow
            );

            await _publisher.PublishAsync(dlq);
        }
    }
}
