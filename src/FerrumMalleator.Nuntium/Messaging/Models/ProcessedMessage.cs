using System.Diagnostics.CodeAnalysis;

namespace FerrumMalleator.Nuntium.Messaging.Models
{
    [ExcludeFromCodeCoverage]
    public sealed class ProcessedMessage(Guid messageId)
    {
        public Guid MessageId { get; } = messageId;
        public DateTime ProcessedAt { get; } = DateTime.UtcNow;
    }
}
