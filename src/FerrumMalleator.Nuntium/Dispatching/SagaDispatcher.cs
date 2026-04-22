using FerrumMalleator.Nuntium.Abstractions;
using FerrumMalleator.Nuntium.Abstractions.Persistence;
using FerrumMalleator.Nuntium.Abstractions.Publishers;
using FerrumMalleator.Nuntium.Builders;
using FerrumMalleator.Nuntium.Messaging.Envelopes;
using FerrumMalleator.Nuntium.Messaging.Models;
using FerrumMalleator.Nuntium.Sagas.Handlers;
using System.Text.Json;

namespace FerrumMalleator.Nuntium.Dispatching
{
    internal sealed class SagaDispatcher(
        IServiceProvider provider,
        SagaHandlerRegistry sagaHandlerRegistry,
        IIdempotencyStore idempotency,
        IRetryExecutor retryExecutor,
        IMessageTransport messageTransport,
        MessageMetadataRegistry messageMetadataRegistry)
    {
        private readonly IServiceProvider _provider = provider;
        private readonly IMessageTransport _messageTransport = messageTransport;
        private readonly MessageMetadataRegistry _messageMetadataRegistry = messageMetadataRegistry;
        private readonly SagaHandlerRegistry _sagaHandlerRegistry = sagaHandlerRegistry;
        private readonly IIdempotencyStore _idempotency = idempotency ?? throw new ArgumentNullException(nameof(idempotency));
        private readonly IRetryExecutor _retryExecutor = retryExecutor;

        public async Task DispatchAsync(IMessageEnvelope envelope, Type messageType, object payload, CancellationToken ct)
        {
            if (await _idempotency.HasProcessedAsync(envelope.MessageId, ct))
                return;

            var descriptor = _sagaHandlerRegistry.Get(messageType);

            var correlationId = ResolveCorrelationId(payload, messageType);

            var state = await descriptor.LoadState(_provider, correlationId.ToString()!, ct);

            if (state == null)
            {
                state = Activator.CreateInstance(descriptor.StateType);

                descriptor.StateType
                    .GetProperty("CorrelationId")?
                    .SetValue(state, correlationId);
            }

            await _retryExecutor.ExecuteAsync(
                async () =>
                {
                    await descriptor.Invoker.Invoke(
                        payload,
                        state!,
                        _provider,
                        ct);
                },
                async (ex, retryCount) =>
                {
                    var dlqMessage = new DeadLetterMessage
                    {
                        MessageId = envelope.MessageId,
                        MessageType = envelope.MessageType,
                        PayloadJson = JsonSerializer.Serialize(envelope),
                        Error = ex.Message,
                        StackTrace = ex.StackTrace ?? string.Empty,
                        RetryCount = retryCount,
                        FailedAt = DateTime.UtcNow
                    };

                    var metadata = _messageMetadataRegistry.Get<DeadLetterMessage>();

                    var dlqEnvelope = new MessageEnvelope<DeadLetterMessage>
                    {
                        MessageId = Guid.NewGuid(),
                        MessageType = metadata.Key,
                        Payload = dlqMessage
                    };

                    var json = JsonSerializer.Serialize(dlqEnvelope);

                    await _messageTransport.SendAsync(metadata.Key, json, ct);
                });

            await descriptor.SaveState(_provider, state!, ct);

            await _idempotency.MarkProcessedAsync(envelope.MessageId, ct);
        }

        private object ResolveCorrelationId(object message, Type messageType)
        {
            var customResolverType = typeof(ISagaCorrelation<>).MakeGenericType(messageType);

            var customResolver = _provider.GetService(customResolverType);

            if (customResolver != null)
            {
                var method = customResolverType.GetMethod("GetCorrelationId");
                return method!.Invoke(customResolver, [message])!;
            }

            var type = message.GetType();
            var prop = type.GetProperty("CorrelationId");

            if (prop != null)
                return prop.GetValue(message)!;

            var idProp = type
                .GetProperties()
                .FirstOrDefault(p => p.Name.EndsWith("Id", StringComparison.OrdinalIgnoreCase));

            if (idProp != null)
                return idProp.GetValue(message)!;

            var fallback = type.GetProperty("Id");

            if (fallback != null)
                return fallback.GetValue(message)!;

            throw new InvalidOperationException(
                $"No correlation id found for message {type.Name}. " +
                $"Expected property: CorrelationId, *Id or Id. " +
                $"Or implement ISagaCorrelation<{type.Name}>.");
        }
    }
}