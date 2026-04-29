using FerrumMalleator.Nuntium.Abstractions.Publishers;
using System.Text.Json;

namespace FerrumMalleator.Nuntium.Transport.Default
{
    internal sealed class DefaultMessagePublisher(IMessageTransport transport) : IMessagePublisher
    {
        public Task PublishAsync<T>(T message, CancellationToken ct = default)
        {
            var payload = JsonSerializer.Serialize(message);

            return transport.SendAsync(typeof(T).Name, payload, ct);
        }
    }
}
