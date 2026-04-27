using FerrumMalleator.Nuntium.Abstractions.Publishers;
using FerrumMalleator.Nuntium.Dispatching;
using Microsoft.Extensions.DependencyInjection;
using System.Diagnostics.CodeAnalysis;

namespace FerrumMalleator.Nuntium.Transport.InMemory
{
    [ExcludeFromCodeCoverage]
    internal sealed class InMemoryTransport(IServiceProvider provider) : IMessageTransport
    {
        private readonly IServiceProvider _provider = provider;

        public async Task SendAsync(string messageType, string payload, CancellationToken ct)
        {
            using var scope = _provider.CreateScope();

            var dispatcher = scope.ServiceProvider.GetRequiredService<MessageDispatcher>();

            await dispatcher.DispatchAsync(payload, ct);
        }
    }
}
