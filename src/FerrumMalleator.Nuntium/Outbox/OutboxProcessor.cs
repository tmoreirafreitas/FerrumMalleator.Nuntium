using FerrumMalleator.Nuntium.Abstractions;
using FerrumMalleator.Nuntium.Abstractions.Persistence;
using FerrumMalleator.Nuntium.Abstractions.Publishers;
using FerrumMalleator.Nuntium.Builders;
using FerrumMalleator.Nuntium.Diagnostics;
using FerrumMalleator.Nuntium.Messaging.Envelopes;
using FerrumMalleator.Nuntium.Messaging.Models;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using System.Diagnostics;
using System.Text.Json;

namespace FerrumMalleator.Nuntium.Outbox
{
    internal sealed class OutboxProcessor(IServiceProvider provider) : BackgroundService
    {
        private readonly IServiceProvider _provider = provider;

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                await ProcessOnceAsync(stoppingToken);

                await Task.Delay(1000, stoppingToken);
            }
        }

        public async Task ProcessOnceAsync(CancellationToken stoppingToken)
        {
            using var activity = NuntiumDiagnostics.ActivitySource.StartActivity("nuntium.outbox.process", ActivityKind.Internal);

            var start = Stopwatch.GetTimestamp();

            activity?.SetTag("messaging.system", "nuntium");

            activity?.SetTag("messaging.operation", "outbox.process");

            try
            {
                using var scope = _provider.CreateScope();

                var store = scope.ServiceProvider.GetRequiredService<IOutboxStore>();

                var transport = scope.ServiceProvider.GetRequiredService<IMessageTransport>();

                var registry = scope.ServiceProvider.GetRequiredService<MessageMetadataRegistry>();

                var retryExecutor = scope.ServiceProvider.GetRequiredService<IRetryExecutor>();

                var messages = await store.GetPendingAsync(50, stoppingToken);

                activity?.SetTag("outbox.batch.size", messages.Count);

                foreach (var msg in messages)
                {
                    using var messageActivity = NuntiumDiagnostics.ActivitySource.StartActivity("nuntium.outbox.message.process", ActivityKind.Producer);

                    messageActivity?.SetTag("messaging.message_id", msg.Id);

                    messageActivity?.SetTag("messaging.message_type", msg.Type);

                    var metadata = registry.Get(msg.Type);

                    var messageType = metadata.Type;

                    var envelopeType = typeof(MessageEnvelope<>).MakeGenericType(messageType);

                    var envelope = JsonSerializer.Deserialize(msg.Payload, envelopeType)
                        ?? throw new InvalidOperationException(
                            "Failed to deserialize envelope");

                    await retryExecutor.ExecuteAsync(
                        async () =>
                        {
                            messageActivity?.AddEvent(new ActivityEvent("outbox.message.publish"));

                            await transport.SendAsync(msg.Type, msg.Payload, stoppingToken);

                            await store.MarkProcessedAsync(msg.Id, stoppingToken);

                            messageActivity?.AddEvent(new ActivityEvent("outbox.message.processed"));

                            messageActivity?.SetStatus(ActivityStatusCode.Ok);

                            NuntiumDiagnostics.OutboxMessagesProcessed.Add(1);

                            NuntiumDiagnostics.MessagesPublished.Add(1);
                        },
                        async (ex, retryCount) =>
                        {
                            messageActivity?.SetStatus(ActivityStatusCode.Error);

                            messageActivity?.AddException(ex);

                            messageActivity?.AddEvent(new ActivityEvent("outbox.retry",
                                tags: new ActivityTagsCollection
                                {
                                    { "retry.count", retryCount }
                                }));

                            NuntiumDiagnostics.OutboxFailures.Add(1);

                            var dlqMessage = new DeadLetterMessage
                            {
                                MessageId = msg.Id,
                                MessageType = msg.Type,
                                PayloadJson = msg.Payload,
                                Error = ex.Message,
                                StackTrace = ex.StackTrace ?? string.Empty,
                                RetryCount = retryCount
                            };

                            var metadata = registry.Get<DeadLetterMessage>();

                            var envelope = new MessageEnvelope<DeadLetterMessage>
                            {
                                MessageId = Guid.NewGuid(),
                                MessageType = metadata.Key,
                                Payload = dlqMessage
                            };

                            var json = JsonSerializer.Serialize(envelope);

                            messageActivity?.AddEvent(new ActivityEvent("outbox.message.deadlettered"));

                            NuntiumDiagnostics.MessagesDeadlettered.Add(1);

                            await transport.SendAsync(metadata.Key, json, stoppingToken);

                            await store.MarkFailedAsync(msg.Id, ex.Message, stoppingToken);
                        });
                }

                activity?.SetStatus(ActivityStatusCode.Ok);
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

                NuntiumDiagnostics.OutboxProcessDuration.Record(elapsed.TotalMilliseconds);
            }
        }
    }
}