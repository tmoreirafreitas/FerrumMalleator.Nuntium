using FerrumMalleator.Nuntium.Abstractions;
using Microsoft.Extensions.DependencyInjection;
using System.Diagnostics.CodeAnalysis;

namespace FerrumMalleator.Nuntium.Sagas.Handlers
{
    [ExcludeFromCodeCoverage]
    internal sealed class SagaHandlerInvoker<TMessage, TState> : ISagaHandlerInvoker where TState : ISagaState
    {
        public async Task Invoke(object message, object state, IServiceProvider provider, CancellationToken ct)
        {
            var handler = provider.GetRequiredService<ISagaHandler<TMessage, TState>>();

            await handler.Handle((TMessage)message, (TState)state, ct);
        }
    }
}
