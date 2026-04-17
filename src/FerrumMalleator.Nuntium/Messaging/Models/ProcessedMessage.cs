using System;
namespace FerrumMalleator.Nuntium.Messaging.Models
{
    public sealed class ProcessedMessage(Guid messageId)
    {
        public Guid MessageId { get; } = messageId;
        public DateTime ProcessedAt { get; } = DateTime.UtcNow;
    }
}
