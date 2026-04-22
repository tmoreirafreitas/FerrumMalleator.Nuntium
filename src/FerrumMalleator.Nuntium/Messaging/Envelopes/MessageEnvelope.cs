namespace FerrumMalleator.Nuntium.Messaging.Envelopes
{
    public class MessageEnvelope<T> : BaseEnvelope, IMessageEnvelope 
        where T : class
    {
        public object Payload { get; set; } = default!;
    }
}
