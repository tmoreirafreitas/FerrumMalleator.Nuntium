using FerrumMalleator.Nuntium.Abstractions;
using FerrumMalleator.Nuntium.Abstractions.Publishers;
using FerrumMalleator.Nuntium.Builders;
using FerrumMalleator.Nuntium.Diagnostics;
using FerrumMalleator.Nuntium.Messaging.Envelopes;
using FerrumMalleator.Nuntium.Messaging.Models;
using FerrumMalleator.Nuntium.Sagas.Handlers;
using Microsoft.Extensions.DependencyInjection;
using System.Diagnostics;
using System.Text.Json;

namespace FerrumMalleator.Nuntium.Dispatching
{
    internal sealed class MessageDispatcher(
        IServiceProvider provider,
        MessageMetadataRegistry messageTypeRegistry,
        ConsumerInvokerRegistry consumerInvokerRegistry,
        SagaHandlerRegistry? sagaRegistry = null)
    {
        private readonly IServiceProvider _provider = provider;
        private readonly SagaHandlerRegistry? _sagaRegistry = sagaRegistry;
        private readonly MessageMetadataRegistry _messageTypeRegistry = messageTypeRegistry;
        private readonly ConsumerInvokerRegistry _consumerInvokerRegistry = consumerInvokerRegistry;
        private static readonly JsonSerializerOptions JsonSerializerOptions = new() { PropertyNameCaseInsensitive = true };

        public async Task DispatchAsync(string json, CancellationToken ct)
        {
            var start = Stopwatch.GetTimestamp();

            var meta = JsonSerializer.Deserialize<BaseEnvelope>(json);

            using var activity = NuntiumDiagnostics.ActivitySource.StartActivity("nuntium.message.dispatch", ActivityKind.Consumer);

            activity?.SetTag("messaging.system", "nuntium");
            activity?.SetTag("messaging.operation", "process");
            activity?.SetTag("messaging.message_type", meta?.MessageType);
            activity?.SetTag("messaging.message_id", meta?.MessageId);

            try
            {
                var metadata = _messageTypeRegistry.Get(meta!.MessageType);
                var messageType = metadata.Type;

                activity?.SetTag("messaging.destination.name", metadata.Key);

                var envelopeType = typeof(MessageEnvelope<>).MakeGenericType(messageType);

                var envelope = JsonSerializer.Deserialize(json, envelopeType);

                var payloadProperty = envelopeType.GetProperty("Payload")!;

                var rawPayload = payloadProperty.GetValue(envelope)!;

                object payload = rawPayload is JsonElement jsonElement
                        ? jsonElement.Deserialize(messageType, JsonSerializerOptions)!
                        : rawPayload;

                var retry = _provider.GetRequiredService<IRetryExecutor>();

                var transport = _provider.GetRequiredService<IMessageTransport>();

                var registry = _provider.GetRequiredService<MessageMetadataRegistry>();

                await retry.ExecuteAsync(
                    async () =>
                    {
                        if (_consumerInvokerRegistry.HasHandler(messageType))
                        {
                            activity?.AddEvent(new ActivityEvent("consumer.dispatch"));

                            await _consumerInvokerRegistry.Invoke(messageType, _provider, payload, ct);
                        }

                        if (_sagaRegistry != null && _sagaRegistry.Contains(messageType))
                        {
                            activity?.AddEvent(new ActivityEvent("saga.dispatch"));

                            var sagaDispatcher = _provider.GetRequiredService<SagaDispatcher>();

                            await sagaDispatcher.DispatchAsync((IMessageEnvelope)envelope!, messageType, payload, ct);
                        }

                        NuntiumDiagnostics.MessagesDispatched.Add(1);
                    },
                    async (ex, retryCount) =>
                    {
                        activity?.AddEvent(new ActivityEvent("message.retry", tags: new ActivityTagsCollection { { "retry.count", retryCount } }));

                        activity?.SetStatus(ActivityStatusCode.Error);

                        activity?.AddException(ex);

                        var baseEnvelope = (IMessageEnvelope)envelope!;

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

                        activity?.AddEvent(new ActivityEvent("message.deadlettered"));

                        NuntiumDiagnostics.MessagesDeadlettered.Add(1);

                        await transport.SendAsync(dlqMetadata.Key, dlqJson, ct);
                    });

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

                NuntiumDiagnostics.DispatchDuration.Record(elapsed.TotalMilliseconds);
            }
        }
    }
}