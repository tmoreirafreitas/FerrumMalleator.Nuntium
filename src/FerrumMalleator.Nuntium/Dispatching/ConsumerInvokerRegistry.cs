using FerrumMalleator.Nuntium.Abstractions.Consumers;
using FerrumMalleator.Nuntium.Diagnostics;
using Microsoft.Extensions.DependencyInjection;
using System.Diagnostics;

namespace FerrumMalleator.Nuntium.Dispatching
{
    internal sealed class ConsumerInvokerRegistry
    {
        private readonly Dictionary<Type, Func<IServiceProvider, object, CancellationToken, Task>> _invokers = [];

        public void Register<TMessage>()
        {
            _invokers[typeof(TMessage)] =
                async (provider, payload, ct) =>
                {
                    var consumers = provider.GetServices<IMessageConsumer<TMessage>>();

                    foreach (var consumer in consumers)
                    {
                        var consumerName = consumer.GetType().Name;
                        var messageType = typeof(TMessage).Name;

                        using var activity = NuntiumDiagnostics.ActivitySource.StartActivity("nuntium.consumer.consume", ActivityKind.Consumer);

                        activity?.SetTag("messaging.system", "nuntium");
                        activity?.SetTag("messaging.operation", "process");
                        activity?.SetTag("messaging.message_type", messageType);
                        activity?.SetTag("messaging.consumer", consumerName);

                        var start = Stopwatch.GetTimestamp();

                        try
                        {
                            await consumer.ConsumeAsync((TMessage)payload, ct).ConfigureAwait(false);

                            NuntiumDiagnostics.MessagesConsumed.Add(1);
                        }
                        catch (Exception ex)
                        {
                            activity?.SetStatus(ActivityStatusCode.Error);
                            activity?.AddException(ex);

                            NuntiumDiagnostics.MessagesFailed.Add(1);

                            throw;
                        }
                        finally
                        {
                            var elapsed = Stopwatch.GetElapsedTime(start);
                            NuntiumDiagnostics.ConsumerDuration.Record(elapsed.TotalMilliseconds);
                        }
                    }
                };
        }

        public Task Invoke(Type type, IServiceProvider provider, object payload, CancellationToken ct)
        {
            return _invokers[type](provider, payload, ct);
        }

        public bool HasHandler(Type type)
        {
            return _invokers.ContainsKey(type);
        }
    }
}