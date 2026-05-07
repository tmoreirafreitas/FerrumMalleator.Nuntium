using FerrumMalleator.Nuntium.Abstractions;
using FerrumMalleator.Nuntium.Abstractions.Persistence;
using FerrumMalleator.Nuntium.Abstractions.Publishers;
using FerrumMalleator.Nuntium.Builders;
using FerrumMalleator.Nuntium.Diagnostics;
using FerrumMalleator.Nuntium.Messaging.Envelopes;
using FerrumMalleator.Nuntium.Messaging.Models;
using FerrumMalleator.Nuntium.Sagas.Handlers;
using System.Diagnostics;
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
        private readonly IIdempotencyStore _idempotency = idempotency;
        private readonly IRetryExecutor _retryExecutor = retryExecutor;

        public async Task DispatchAsync(IMessageEnvelope envelope, Type messageType, object payload, CancellationToken ct)
        {
            var start = Stopwatch.GetTimestamp();

            using var activity = NuntiumDiagnostics.ActivitySource.StartActivity("nuntium.saga.dispatch", ActivityKind.Internal);

            activity?.SetTag("messaging.system", "nuntium");
            activity?.SetTag("messaging.operation", "saga");
            activity?.SetTag("messaging.message_type", envelope.MessageType);
            activity?.SetTag("messaging.message_id", envelope.MessageId);

            try
            {
                if (await _idempotency.HasProcessedAsync(envelope.MessageId, ct))
                {
                    activity?.AddEvent(new ActivityEvent("saga.idempotent.skip"));

                    return;
                }

                var descriptor = _sagaHandlerRegistry.Get(messageType);

                var correlationId = ResolveCorrelationId(payload, messageType);

                activity?.SetTag("messaging.conversation_id", correlationId.ToString());

                var state = await descriptor.LoadState(_provider, correlationId.ToString()!, ct);

                if (state == null)
                {
                    activity?.AddEvent(new ActivityEvent("saga.state.created"));

                    state = Activator.CreateInstance(descriptor.StateType);

                    descriptor.StateType.GetProperty("CorrelationId")?.SetValue(state, correlationId);
                }
                else
                {
                    activity?.AddEvent(new ActivityEvent("saga.state.loaded"));
                }

                await _retryExecutor.ExecuteAsync(
                    async () =>
                    {
                        activity?.AddEvent(new ActivityEvent("saga.handler.invoke"));

                        await descriptor.Invoker.Invoke(payload, state!, _provider, ct);

                        NuntiumDiagnostics.SagaExecutions.Add(1);
                    },
                    async (ex, retryCount) =>
                    {
                        activity?.AddEvent(new ActivityEvent("saga.retry",
                                tags: new ActivityTagsCollection
                                {
                                    { "retry.count", retryCount }
                                }));

                        activity?.SetStatus(ActivityStatusCode.Error);

                        activity?.AddException(ex);

                        NuntiumDiagnostics.SagaFailures.Add(1);

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

                        activity?.AddEvent(new ActivityEvent("saga.deadlettered"));

                        NuntiumDiagnostics.MessagesDeadlettered.Add(1);

                        await _messageTransport.SendAsync(metadata.Key, json, ct);
                    });

                activity?.AddEvent(new ActivityEvent("saga.state.saved"));

                await descriptor.SaveState(_provider, state!, ct);

                await _idempotency.MarkProcessedAsync(envelope.MessageId, ct);

                activity?.SetStatus(ActivityStatusCode.Ok);
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

                NuntiumDiagnostics.SagaDuration.Record(elapsed.TotalMilliseconds);
            }
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

            var idProp = type.GetProperties().FirstOrDefault(p => p.Name.EndsWith("Id", StringComparison.OrdinalIgnoreCase));

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