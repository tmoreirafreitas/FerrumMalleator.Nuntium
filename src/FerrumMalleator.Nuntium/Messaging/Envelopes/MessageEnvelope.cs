namespace FerrumMalleator.Nuntium.Messaging.Envelopes
{
    public class MessageEnvelope<T> : BaseEnvelope, IMessageEnvelope
    {
        public object Payload { get; set; } = default!;
    }
}
