using FerrumMalleator.Nuntium.Abstractions;
using FerrumMalleator.Nuntium.Diagnostics;
using Microsoft.Extensions.DependencyInjection;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;

namespace FerrumMalleator.Nuntium.Sagas.Handlers
{
    [ExcludeFromCodeCoverage]
    internal sealed class SagaHandlerInvoker<TMessage, TState> : ISagaHandlerInvoker where TState : ISagaState
    {
        public async Task Invoke(object message, object state, IServiceProvider provider, CancellationToken ct)
        {
            using var activity = NuntiumDiagnostics.ActivitySource.StartActivity("nuntium.saga.handler.invoke", ActivityKind.Internal);

            var start = Stopwatch.GetTimestamp();

            activity?.SetTag("messaging.system", "nuntium");

            activity?.SetTag("messaging.operation", "saga.handler");

            activity?.SetTag("messaging.message_type", typeof(TMessage).Name);

            activity?.SetTag("saga.state_type", typeof(TState).Name);

            try
            {
                var handler = provider.GetRequiredService<ISagaHandler<TMessage, TState>>();

                activity?.SetTag("saga.handler", handler.GetType().Name);

                await handler.HandleAsync((TMessage)message, (TState)state, ct);

                activity?.SetStatus(ActivityStatusCode.Ok);

                NuntiumDiagnostics.SagaExecutions.Add(1);
            }
            catch (Exception ex)
            {
                activity?.SetStatus(ActivityStatusCode.Error);

                activity?.AddException(ex);

                NuntiumDiagnostics.SagaFailures.Add(1);

                throw;
            }
            finally
            {
                var elapsed = Stopwatch.GetElapsedTime(start);

                NuntiumDiagnostics.SagaHandlerDuration.Record(elapsed.TotalMilliseconds);
            }
        }
    }
}