using FerrumMalleator.Nuntium.Abstractions;
using FerrumMalleator.Nuntium.Abstractions.Publishers;
using FerrumMalleator.Nuntium.Builders;
using FerrumMalleator.Nuntium.Messaging.Envelopes;
using FerrumMalleator.Nuntium.Messaging.Models;
using FerrumMalleator.Nuntium.Sagas.Handlers;
using Microsoft.Extensions.DependencyInjection;
using System.Text.Json;

namespace FerrumMalleator.Nuntium.Dispatching
{
    internal sealed class MessageDispatcher(IServiceProvider provider,
        MessageMetadataRegistry messageTypeRegistry,
        ConsumerInvokerRegistry consumerInvokerRegistry,
        SagaHandlerRegistry? sagaRegistry = null)
    {
        private readonly IServiceProvider _provider = provider;
        private readonly SagaHandlerRegistry? _sagaRegistry = sagaRegistry;
        private readonly MessageMetadataRegistry _messageTypeRegistry = messageTypeRegistry;
        private readonly ConsumerInvokerRegistry _consumerInvokerRegistry = consumerInvokerRegistry;

        public async Task DispatchAsync(string json, CancellationToken ct)
        {
            var meta = JsonSerializer.Deserialize<BaseEnvelope>(json)
                ?? throw new InvalidOperationException();

            var metadata = _messageTypeRegistry.Get(meta.MessageType);
            var messageType = metadata.Type;

            var envelopeType = typeof(MessageEnvelope<>).MakeGenericType(messageType);

            var envelope = JsonSerializer.Deserialize(json, envelopeType)
                ?? throw new InvalidOperationException();

            var payloadProperty = envelopeType.GetProperty("Payload")!;
            var rawPayload = payloadProperty.GetValue(envelope)!;

            object payload = rawPayload is JsonElement jsonElement
                ? jsonElement.Deserialize(messageType, new JsonSerializerOptions
                {
                    PropertyNameCaseInsensitive = true
                })!
                : rawPayload;

            var retry = _provider.GetRequiredService<IRetryExecutor>();
            var transport = _provider.GetRequiredService<IMessageTransport>();
            var registry = _provider.GetRequiredService<MessageMetadataRegistry>();

            await retry.ExecuteAsync(
                async () =>
                {
                    if (_consumerInvokerRegistry.HasHandler(messageType))
                    {
                        await _consumerInvokerRegistry.Invoke(
                            messageType,
                            _provider,
                            payload,
                            ct);
                    }

                    if (_sagaRegistry != null && _sagaRegistry.Contains(messageType))
                    {
                        var sagaDispatcher = _provider.GetRequiredService<SagaDispatcher>();

                        await sagaDispatcher.DispatchAsync(
                            (IMessageEnvelope)envelope,
                            messageType,
                            payload,
                            ct);
                    }
                },
                async (ex, retryCount) =>
                {
                    var baseEnvelope = (IMessageEnvelope)envelope;

                    var dlqMessage = new DeadLetterMessage
                    {
                        MessageId = baseEnvelope.MessageId,
                        MessageType = baseEnvelope.MessageType,
                        PayloadJson = json,
                        Error = ex.Message,
                        StackTrace = ex.StackTrace ?? string.Empty,
                        RetryCount = retryCount
                    };

                    var dlqMetadata = registry.Get<DeadLetterMessage>();

                    var dlqEnvelope = new MessageEnvelope<DeadLetterMessage>
                    {
                        MessageId = Guid.NewGuid(),
                        MessageType = dlqMetadata.Key,
                        Payload = dlqMessage
                    };

                    var dlqJson = JsonSerializer.Serialize(dlqEnvelope);

                    await transport.SendAsync(dlqMetadata.Key, dlqJson, ct);
                });
        }
    }
}
