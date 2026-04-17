namespace FerrumMalleator.Nuntium.Messaging.Envelopes
{
    public class BaseEnvelope
    {
        public Guid MessageId { get; set; }
        public string MessageType { get; set; } = default!;
        public DateTime OccurredOn { get; set; }  = DateTime.UtcNow;      
    }
}
