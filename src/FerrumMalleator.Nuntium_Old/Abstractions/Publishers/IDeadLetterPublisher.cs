using FerrumMalleator.Nuntium.Messaging.Envelopes;
using System;
using System.Threading.Tasks;

namespace FerrumMalleator.Nuntium.Abstractions.Publishers
{
    public interface IDeadLetterPublisher
    {
        Task PublishAsync(IMessageEnvelope envelope, Exception ex, int retryCount = 0);
    }
}
