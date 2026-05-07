using FerrumMalleator.Nuntium.Abstractions.Publishers;
using FerrumMalleator.Nuntium.Diagnostics;
using FerrumMalleator.Nuntium.Dispatching;
using Microsoft.Extensions.DependencyInjection;
using System.Diagnostics;

namespace FerrumMalleator.Nuntium.Transport.InMemory
{
    internal sealed class InMemoryTransport(IServiceProvider provider) : IMessageTransport
    {
        private readonly IServiceProvider _provider = provider;

        public async Task SendAsync(string messageType, string payload, CancellationToken ct)
        {
            using var activity = NuntiumDiagnostics.ActivitySource.StartActivity("nuntium.transport.inmemory.send", ActivityKind.Producer);

            var start = Stopwatch.GetTimestamp();

            activity?.SetTag("messaging.system", "nuntium");

            activity?.SetTag("messaging.operation", "transport");

            activity?.SetTag("messaging.destination.kind", "inmemory");

            activity?.SetTag("messaging.message_type", messageType);

            try
            {
                using var scope = _provider.CreateScope();

                var dispatcher = scope.ServiceProvider.GetRequiredService<MessageDispatcher>();

                activity?.AddEvent(new ActivityEvent("transport.dispatch"));

                await dispatcher.DispatchAsync(payload, ct);

                activity?.SetStatus(ActivityStatusCode.Ok);

                NuntiumDiagnostics.MessagesTransported.Add(1);
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

                NuntiumDiagnostics.TransportDuration.Record(elapsed.TotalMilliseconds);
            }
        }
    }
}