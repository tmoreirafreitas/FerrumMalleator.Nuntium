using FerrumMalleator.Nuntium.Abstractions;
using FerrumMalleator.Nuntium.Abstractions.Persistence;
using Microsoft.Extensions.DependencyInjection;

namespace FerrumMalleator.Nuntium.Sagas.Handlers
{
    internal sealed class SagaHandlerRegistry
    {
        private readonly Dictionary<Type, SagaHandlerDescriptor> _descriptors = [];

        public void Register<TMessage, TState>() where TState : ISagaState
        {
            _descriptors[typeof(TMessage)] = new SagaHandlerDescriptor
            {
                MessageType = typeof(TMessage),

                StateType = typeof(TState),

                Invoker = async (message, state, provider, ct) =>
                {
                    var handler = provider.GetRequiredService<ISagaHandler<TMessage, TState>>();

                    await handler.HandleAsync(
                        (TMessage)message,
                        (TState)state,
                        ct);
                },

                LoadState = async (provider, correlationId, ct) =>
                {
                    _ = Guid.TryParse(correlationId, out Guid stateId);
                    var repository = provider.GetRequiredService<ISagaRepository<TState>>();
                    return await repository.GetAsync(stateId, ct);
                },

                SaveState = async (provider, state, ct) =>
                {
                    var repository = provider.GetRequiredService<ISagaRepository<TState>>();
                    await repository.SaveAsync((TState)state, ct);
                }
            };
        }

        public SagaHandlerDescriptor Get(Type messageType)
        {
            return _descriptors[messageType];
        }

        public bool Contains(Type messageType)
        {
            return _descriptors.ContainsKey(messageType);
        }
    }
}
