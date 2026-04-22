namespace FerrumMalleator.Nuntium.Messaging.Envelopes
{
    public class MessageEnvelope<T> : BaseEnvelope, IMessageEnvelope
    {
        public T Payload { get; set; } = default!;
        object IMessageEnvelope.Payload => Payload!;
    }
}
