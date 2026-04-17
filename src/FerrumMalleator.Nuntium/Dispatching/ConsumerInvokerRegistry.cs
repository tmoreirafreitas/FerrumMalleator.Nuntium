using FerrumMalleator.Nuntium.Abstractions.Consumers;
using Microsoft.Extensions.DependencyInjection;

namespace FerrumMalleator.Nuntium.Dispatching
{
    internal sealed class ConsumerInvokerRegistry
    {
        private readonly Dictionary<Type, Func<IServiceProvider, object, CancellationToken, Task>> _invokers = [];

        public void Register<TMessage>()
        {
            _invokers[typeof(TMessage)] =
                async (provider, payload, cancellationToken) =>
                {
                    var consumers = provider.GetServices<IMessageConsumer<TMessage>>();

                    foreach (var consumer in consumers)
                    {
                        await consumer.ConsumeAsync((TMessage)payload, cancellationToken).ConfigureAwait(false);
                    }
                };
        }

        public Task Invoke(Type type, IServiceProvider provider, object payload, CancellationToken cancellationToken)
        {
            return _invokers[type](provider, payload, cancellationToken);
        }

        public bool HasHandler(Type type)
        {
            return _invokers.ContainsKey(type);
        }
    }
}
