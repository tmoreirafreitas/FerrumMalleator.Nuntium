using System.Diagnostics.CodeAnalysis;

namespace FerrumMalleator.Nuntium.Messaging.Envelopes
{
    public class MessageEnvelope<T> : BaseEnvelope, IMessageEnvelope
    {
        public T Payload { get; set; } = default!;

        [ExcludeFromCodeCoverage]
        object IMessageEnvelope.Payload => Payload!;
    }
}
