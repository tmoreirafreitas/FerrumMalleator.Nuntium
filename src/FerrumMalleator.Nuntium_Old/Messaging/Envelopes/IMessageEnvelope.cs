using System;
namespace FerrumMalleator.Nuntium.Messaging.Envelopes
{
    public interface IMessageEnvelope
    {
        Guid MessageId { get; }
        object Payload { get; }
        string MessageType { get; }
        DateTime OccurredOn { get; }
    }
}
