using System;

namespace FerrumMalleator.Nuntium.Messaging.Envelopes
{
    public class MessageEnvelope<T> : BaseEnvelope, IMessageEnvelope
    {
        public object Payload { get; }
        public MessageEnvelope(Guid messageId, string messageType, T Payload) : base(messageId, messageType)
        {
            this.Payload = Payload;
        }
    }
}
