
using System;

namespace FerrumMalleator.Nuntium.Messaging.Envelopes
{
    public class BaseEnvelope
    {
        public Guid MessageId { get; }
        public string MessageType { get; }
        public DateTime OccurredOn { get; }

        public BaseEnvelope(Guid messageId, string messageType)
        {
            MessageId = messageId;
            MessageType = messageType;
            OccurredOn = DateTime.UtcNow;
        }       
    }
}
