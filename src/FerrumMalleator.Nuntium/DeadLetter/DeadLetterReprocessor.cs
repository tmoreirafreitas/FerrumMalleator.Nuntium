using FerrumMalleator.Nuntium.Abstractions.Persistence;
using FerrumMalleator.Nuntium.Abstractions.Publishers;
using FerrumMalleator.Nuntium.Diagnostics;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using System.Diagnostics;

namespace FerrumMalleator.Nuntium.DeadLetter
{
    internal sealed class DeadLetterReprocessor(IServiceProvider provider) : BackgroundService
    {
        private readonly IServiceProvider _provider = provider;

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                await ProcessOnceAsync(stoppingToken);

                await Task.Delay(5000, stoppingToken);
            }
        }

        public async Task ProcessOnceAsync(CancellationToken stoppingToken)
        {
            using var activity = NuntiumDiagnostics.ActivitySource.StartActivity("nuntium.deadletter.reprocess", ActivityKind.Internal);

            var start = Stopwatch.GetTimestamp();

            activity?.SetTag("messaging.system", "nuntium");
            activity?.SetTag("messaging.operation", "deadletter.reprocess");

            try
            {
                using var scope = _provider.CreateScope();

                var store = scope.ServiceProvider.GetRequiredService<IDeadLetterStore>();

                var transport = scope.ServiceProvider.GetRequiredService<IMessageTransport>();

                var messages = await store.GetPendingAsync(50, stoppingToken);

                activity?.SetTag("deadletter.batch.size", messages.Count);

                foreach (var msg in messages)
                {
                    if (msg.NextRetryAt.HasValue && msg.NextRetryAt > DateTime.UtcNow)
                    {
                        activity?.AddEvent(new ActivityEvent("deadletter.retry.delayed"));

                        continue;
                    }

                    using var messageActivity = NuntiumDiagnostics.ActivitySource.StartActivity("nuntium.deadletter.message.reprocess", ActivityKind.Internal);

                    messageActivity?.SetTag("messaging.message_id", msg.MessageId);

                    messageActivity?.SetTag("messaging.message_type", msg.MessageType);

                    messageActivity?.SetTag("deadletter.retry_count", msg.ReprocessCount);

                    try
                    {
                        await transport.SendAsync(msg.MessageType, msg.PayloadJson, stoppingToken);

                        await store.MarkReprocessedAsync(msg.MessageId, stoppingToken);

                        messageActivity?.SetStatus(ActivityStatusCode.Ok);

                        messageActivity?.AddEvent(new ActivityEvent("deadletter.reprocessed"));

                        NuntiumDiagnostics.MessagesReprocessed.Add(1);
                    }
                    catch (Exception ex)
                    {
                        messageActivity?.SetStatus(ActivityStatusCode.Error);

                        messageActivity?.AddException(ex);

                        NuntiumDiagnostics.MessagesReprocessFailed.Add(1);

                        msg.ReprocessCount++;

                        if (msg.ReprocessCount >= 5)
                        {
                            messageActivity?.AddEvent(new ActivityEvent("deadletter.max_retries_reached"));

                            await store.MarkReprocessedAsync(msg.MessageId, stoppingToken);

                            continue;
                        }

                        var delay = TimeSpan.FromSeconds(Math.Pow(2, msg.ReprocessCount));

                        msg.NextRetryAt = DateTime.UtcNow.Add(delay);

                        messageActivity?.AddEvent(new ActivityEvent("deadletter.retry.scheduled",
                            tags: new ActivityTagsCollection
                            {
                                { "retry.count", msg.ReprocessCount },
                                { "retry.delay.ms", delay.TotalMilliseconds }
                            }));
                    }
                }

                activity?.SetStatus(ActivityStatusCode.Ok);
            }
            catch (Exception ex)
            {
                activity?.SetStatus(ActivityStatusCode.Error);

                activity?.AddException(ex);

                throw;
            }
            finally
            {
                var elapsed = Stopwatch.GetElapsedTime(start);

                NuntiumDiagnostics.DeadLetterReprocessDuration.Record(elapsed.TotalMilliseconds);
            }
        }
    }
}