using FerrumMalleator.Nuntium.Abstractions.Publishers;
using FerrumMalleator.Nuntium.Diagnostics;
using System.Diagnostics;
using System.Text.Json;

namespace FerrumMalleator.Nuntium.Transport.Default
{
    internal sealed class DefaultMessagePublisher(IMessageTransport transport) : IMessagePublisher
    {
        public async Task PublishAsync<T>(T message, CancellationToken ct = default)
        {
            using var activity = NuntiumDiagnostics.ActivitySource.StartActivity("nuntium.message.publish", ActivityKind.Producer);

            var start = Stopwatch.GetTimestamp();

            activity?.SetTag("messaging.system", "nuntium");

            activity?.SetTag("messaging.operation", "publish");

            activity?.SetTag("messaging.message_type", typeof(T).Name);

            activity?.SetTag("messaging.destination.name", typeof(T).Name);

            try
            {
                var payload = JsonSerializer.Serialize(message);

                activity?.AddEvent(new ActivityEvent("message.serialized"));

                await transport.SendAsync(typeof(T).Name, payload, ct);

                activity?.AddEvent(new ActivityEvent("message.sent"));

                activity?.SetStatus(ActivityStatusCode.Ok);

                NuntiumDiagnostics.MessagesPublished.Add(1);
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

                NuntiumDiagnostics.PublishDuration.Record(elapsed.TotalMilliseconds);
            }
        }
    }
}