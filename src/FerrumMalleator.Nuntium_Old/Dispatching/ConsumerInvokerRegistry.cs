using FerrumMalleator.Nuntium.Abstractions.Consumers;
using Microsoft.Extensions.DependencyInjection;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace FerrumMalleator.Nuntium.Dispatching
{
    public sealed class ConsumerInvokerRegistry
    {
        private readonly Dictionary<Type, Func<IServiceProvider, object, CancellationToken, Task>> _invokers = 
            new Dictionary<Type, Func<IServiceProvider, object, CancellationToken, Task>>();

        public void Register<TMessage>()
        {
            _invokers[typeof(TMessage)] =
                async (provider, payload, cancellationToken) =>
                {
                    var consumers =
                        provider.GetServices<IMessageConsumer<TMessage>>();

                    foreach (var consumer in consumers)
                    {
                        await consumer.ConsumeAsync((TMessage)payload, cancellationToken);
                    }
                };
        }

        public Task Invoke(Type type, IServiceProvider provider, object payload, CancellationToken cancellationToken)
        {
            return _invokers[type](provider, payload, cancellationToken);
        }
    }
}
