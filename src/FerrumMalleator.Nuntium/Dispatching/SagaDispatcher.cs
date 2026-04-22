using FerrumMalleator.Nuntium.Abstractions;
using FerrumMalleator.Nuntium.Abstractions.Persistence;
using FerrumMalleator.Nuntium.Abstractions.Publishers;
using FerrumMalleator.Nuntium.Builders;
using FerrumMalleator.Nuntium.Messaging.Envelopes;
using FerrumMalleator.Nuntium.Messaging.Models;
using FerrumMalleator.Nuntium.Sagas.Handlers;
using Microsoft.Extensions.DependencyInjection;
using System.Text.Json;

namespace FerrumMalleator.Nuntium.Dispatching
{
    internal sealed class SagaDispatcher(IServiceProvider provider, SagaHandlerRegistry registry, IIdempotencyStore idempotency, IRetryExecutor retryExecutor)
    {
        private readonly IServiceProvider _provider = provider;
        private readonly SagaHandlerRegistry _registry = registry;
        private readonly IIdempotencyStore _idempotency = idempotency ?? throw new ArgumentNullException(nameof(idempotency));
        private readonly IRetryExecutor _retryExecutor = retryExecutor;

        public async Task DispatchAsync(IMessageEnvelope envelope, Type messageType, object payload, CancellationToken cancellationToken)
        {
            if (await _idempotency.HasProcessedAsync(envelope.MessageId, cancellationToken))
                return;

            var descriptor = _registry.Get(messageType);

            var correlationId = ResolveCorrelationId(payload, messageType);

            var state = await descriptor.LoadState(_provider, correlationId.ToString()!, cancellationToken);

            if (state == null)
            {
                state = Activator.CreateInstance(descriptor.StateType);

                descriptor.StateType
                    .GetProperty("CorrelationId")?
                    .SetValue(state, correlationId);
            }

            var transport = _provider.GetRequiredService<IMessageTransport>();
            var registry = _provider.GetRequiredService<MessageMetadataRegistry>();

            await _retryExecutor.ExecuteAsync(
                async () =>
                {
                    await descriptor.Invoker.Invoke(
                        payload,
                        state!,
                        _provider,
                        cancellationToken);
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

                    var metadata = registry.Get<DeadLetterMessage>();

                    var dlqEnvelope = new MessageEnvelope<DeadLetterMessage>
                    {
                        MessageId = Guid.NewGuid(),
                        MessageType = metadata.Key,
                        Payload = dlqMessage
                    };

                    var json = JsonSerializer.Serialize(dlqEnvelope);

                    await transport.SendAsync(metadata.Key, json, cancellationToken);
                });

            await descriptor.SaveState(_provider, state!, cancellationToken);

            await _idempotency.MarkProcessedAsync(envelope.MessageId, cancellationToken);
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