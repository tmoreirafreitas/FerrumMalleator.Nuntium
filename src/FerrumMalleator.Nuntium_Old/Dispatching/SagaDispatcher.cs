using FerrumMalleator.Nuntium.Abstractions;
using FerrumMalleator.Nuntium.Abstractions.Persistence;
using FerrumMalleator.Nuntium.Abstractions.Publishers;
using FerrumMalleator.Nuntium.Configuration;
using FerrumMalleator.Nuntium.Messaging.Envelopes;
using FerrumMalleator.Nuntium.Sagas.Handlers;
using Microsoft.Extensions.DependencyInjection;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace FerrumMalleator.Nuntium.Dispatching
{
    internal sealed class SagaDispatcher
    {
        private readonly IServiceProvider _provider;
        private readonly SagaHandlerRegistry _registry;
        private readonly IIdempotencyStore _idempotency;
        private readonly RetryPolicyOptions _retryPolicyOptions;

        public SagaDispatcher(IServiceProvider provider,
            SagaHandlerRegistry registry,
            IIdempotencyStore idempotency,
            RetryPolicyOptions retryPolicyOptions)
        {
            _provider = provider;
            _registry = registry;
            _idempotency = idempotency ?? throw new ArgumentNullException(nameof(registry));
            _retryPolicyOptions = retryPolicyOptions;
        }
        public async Task DispatchAsync(IMessageEnvelope envelope, Type messageType, CancellationToken cancellationToken)
        {
            if (await _idempotency.HasProcessedAsync(envelope.MessageId, cancellationToken))
                return;

            var descriptor = _registry.Get(messageType);

            var correlationResolver = _provider.GetRequiredService(typeof(ISagaCorrelation<>).MakeGenericType(messageType));

            var correlationMethod = correlationResolver.GetType().GetMethod("GetCorrelationId");

            var correlationId = correlationMethod.Invoke(correlationResolver, new[] { envelope.Payload });

            var state = await descriptor.LoadState(_provider, correlationId.ToString(), cancellationToken);

            if (state == null)
            {
                state = Activator.CreateInstance(descriptor.StateType);

                descriptor.StateType
                    .GetProperty("CorrelationId")
                    .SetValue(state, correlationId);
            }

            await ExecuteWithRetry(
                () => descriptor.Invoker.Invoke(
                    envelope.Payload,
                    state,
                    _provider,
                    cancellationToken),

                async (ex, retryCount) =>
                {
                    var dlq =
                        _provider.GetRequiredService<IDeadLetterPublisher>();

                    await dlq.PublishAsync(
                        envelope,
                        ex,
                        retryCount);
                });

            await descriptor.SaveState(_provider, state, cancellationToken);

            await _idempotency.MarkProcessedAsync(
                envelope.MessageId, cancellationToken);
        }

        private async Task ExecuteWithRetry(Func<Task> action, Func<Exception, int, Task> onFailure)
        {
            var delays = _retryPolicyOptions.Delays;

            for (int i = 0; i < delays.Count; i++)
            {
                try
                {
                    await action();
                    return;
                }
                catch (Exception) when (i < delays.Count - 1)
                {
                    await Task.Delay(delays[i]);
                }
                catch (Exception ex)
                {
                    await onFailure(ex, i + 1);
                    throw;
                }
            }
        }
    }
}