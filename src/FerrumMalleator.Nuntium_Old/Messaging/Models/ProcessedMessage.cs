using System;
namespace FerrumMalleator.Nuntium.Messaging.Models
{
    public sealed class ProcessedMessage
    {
        public Guid MessageId { get; }
        public DateTime ProcessedAt { get; }

        public ProcessedMessage(Guid messageId)
        {
            MessageId = messageId;
            ProcessedAt = DateTime.UtcNow;
        }
    }
}
