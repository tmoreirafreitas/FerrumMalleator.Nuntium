using FerrumMalleator.Nuntium.Abstractions.Publishers;
using FerrumMalleator.Nuntium.Dispatching;
using Microsoft.Extensions.DependencyInjection;

namespace FerrumMalleator.Nuntium.Transport.InMemory
{
    internal sealed class InMemoryTransport(IServiceProvider provider) : IMessageTransport
    {
        private readonly IServiceProvider _provider = provider;

        public async Task SendAsync(string messageType, string payload, CancellationToken cancellationToken)
        {
            using var scope = _provider.CreateScope();

            var dispatcher = scope.ServiceProvider.GetRequiredService<MessageDispatcher>();

            await dispatcher.DispatchAsync(payload, cancellationToken);
        }
    }
}
