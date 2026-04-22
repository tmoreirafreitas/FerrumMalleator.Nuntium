using FerrumMalleator.Nuntium.Builders;
using FerrumMalleator.Nuntium.Messaging.Envelopes;
using FerrumMalleator.Nuntium.Sagas.Handlers;
using Microsoft.Extensions.DependencyInjection;
using System;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace FerrumMalleator.Nuntium.Dispatching
{
    internal sealed class MessageDispatcher
    {
        private readonly IServiceProvider _provider;
        private readonly SagaHandlerRegistry _sagaRegistry = null;
        private readonly MessageMetadataRegistry _messageTypeRegistry;
        private readonly ConsumerInvokerRegistry _consumerInvokerRegistry;

        public MessageDispatcher(IServiceProvider provider,            
            MessageMetadataRegistry messageTypeRegistry,
            ConsumerInvokerRegistry consumerInvokerRegistry,
            SagaHandlerRegistry sagaRegistry = null)
        {
            _provider = provider;
            _sagaRegistry = sagaRegistry;
            _messageTypeRegistry = messageTypeRegistry;
            _consumerInvokerRegistry = consumerInvokerRegistry;
        }
        public async Task DispatchAsync(string json, CancellationToken cancellationToken)
        {
            var meta = JsonSerializer.Deserialize<BaseEnvelope>(json)
                ?? throw new InvalidOperationException();

            var metadata = _messageTypeRegistry.Get(meta.MessageType);

            var messageType = metadata.Type;

            var envelopeType = typeof(MessageEnvelope<>).MakeGenericType(messageType);

            var envelope = JsonSerializer.Deserialize(json, envelopeType) ?? throw new InvalidOperationException();

            var payload = envelopeType.GetProperty("Payload").GetValue(envelope);

            await _consumerInvokerRegistry.Invoke(messageType, _provider, payload, cancellationToken);

            if (_sagaRegistry != null && _sagaRegistry.Contains(messageType))
            {
                var sagaDispatcher = _provider.GetRequiredService<SagaDispatcher>();
                await sagaDispatcher.DispatchAsync((IMessageEnvelope)envelope, messageType, cancellationToken);
            }
        }
    }
}
