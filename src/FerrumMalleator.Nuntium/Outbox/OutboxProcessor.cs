using FerrumMalleator.Nuntium.Abstractions;
using FerrumMalleator.Nuntium.Abstractions.Persistence;
using FerrumMalleator.Nuntium.Abstractions.Publishers;
using FerrumMalleator.Nuntium.Builders;
using FerrumMalleator.Nuntium.Messaging.Envelopes;
using FerrumMalleator.Nuntium.Messaging.Models;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
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
                using var scope = _provider.CreateScope();

                var store = scope.ServiceProvider.GetRequiredService<IOutboxStore>();
                var transport = scope.ServiceProvider.GetRequiredService<IMessageTransport>();
                var registry = scope.ServiceProvider.GetRequiredService<MessageMetadataRegistry>();
                var retryExecutor = scope.ServiceProvider.GetRequiredService<IRetryExecutor>();
                var messages = await store.GetPendingAsync(50, stoppingToken);

                foreach (var msg in messages)
                {
                    var metadata = registry.Get(msg.Type);
                    var messageType = metadata.Type;
                    var envelopeType = typeof(MessageEnvelope<>).MakeGenericType(messageType);
                    var envelope = JsonSerializer.Deserialize(msg.Payload, envelopeType)
                        ?? throw new InvalidOperationException("Failed to deserialize envelope");

                    await retryExecutor.ExecuteAsync(async () =>
                    {
                        await transport.SendAsync(msg.Type, msg.Payload, stoppingToken);
                        await store.MarkProcessedAsync(msg.Id, stoppingToken);
                    },
                    async (ex, retryCount) =>
                    {
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

                        await transport.SendAsync(metadata.Key, json, stoppingToken);
                        await store.MarkFailedAsync(msg.Id, ex.Message, stoppingToken);
                    });
                }

                await Task.Delay(1000, stoppingToken);
            }
        }
    }
}