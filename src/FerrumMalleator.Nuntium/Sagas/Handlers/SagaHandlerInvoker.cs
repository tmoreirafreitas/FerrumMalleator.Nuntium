using FerrumMalleator.Nuntium.Abstractions;
using Microsoft.Extensions.DependencyInjection;

namespace FerrumMalleator.Nuntium.Sagas.Handlers
{
    internal sealed class SagaHandlerInvoker<TMessage, TState> : ISagaHandlerInvoker where TState : class, ISagaState
    {
        public async Task Invoke(object message, object state, IServiceProvider provider, CancellationToken cancellationToken)
        {
            var handler = provider.GetRequiredService<ISagaHandler<TMessage, TState>>();

            await handler.Handle((TMessage)message, (TState)state, cancellationToken);
        }
    }
}
