using FerrumMalleator.Nuntium.Abstractions;
using FerrumMalleator.Nuntium.Abstractions.Persistence;
using FerrumMalleator.Nuntium.Diagnostics;
using Microsoft.Extensions.DependencyInjection;
using System.Diagnostics;

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
                    using var activity = NuntiumDiagnostics.ActivitySource.StartActivity("nuntium.saga.registry.invoke", ActivityKind.Internal);

                    activity?.SetTag("messaging.message_type", typeof(TMessage).Name);

                    activity?.SetTag("saga.state_type", typeof(TState).Name);

                    try
                    {
                        var handler = provider.GetRequiredService<ISagaHandler<TMessage, TState>>();

                        activity?.SetTag("saga.handler", handler.GetType().Name);

                        await handler.HandleAsync((TMessage)message, (TState)state, ct);

                        activity?.SetStatus(ActivityStatusCode.Ok);
                    }
                    catch (Exception ex)
                    {
                        activity?.SetStatus(ActivityStatusCode.Error);

                        activity?.AddException(ex);

                        NuntiumDiagnostics.SagaFailures.Add(1);

                        throw;
                    }
                },

                LoadState = async (provider, correlationId, ct) =>
                {
                    using var activity = NuntiumDiagnostics.ActivitySource.StartActivity("nuntium.saga.state.load", ActivityKind.Internal);

                    activity?.SetTag("saga.correlation_id", correlationId);

                    activity?.SetTag("saga.state_type", typeof(TState).Name);

                    try
                    {
                        _ = Guid.TryParse(correlationId, out Guid stateId);

                        var repository = provider.GetRequiredService<ISagaRepository<TState>>();

                        var state = await repository.GetAsync(stateId, ct);

                        activity?.SetStatus(ActivityStatusCode.Ok);

                        NuntiumDiagnostics.SagaStateLoads.Add(1);

                        return state;
                    }
                    catch (Exception ex)
                    {
                        activity?.SetStatus(ActivityStatusCode.Error);

                        activity?.AddException(ex);

                        NuntiumDiagnostics.SagaFailures.Add(1);

                        throw;
                    }
                },

                SaveState = async (provider, state, ct) =>
                {
                    using var activity = NuntiumDiagnostics.ActivitySource.StartActivity("nuntium.saga.state.save", ActivityKind.Internal);

                    activity?.SetTag("saga.state_type", typeof(TState).Name);

                    try
                    {
                        var repository = provider.GetRequiredService<ISagaRepository<TState>>();

                        await repository.SaveAsync((TState)state, ct);

                        activity?.SetStatus(ActivityStatusCode.Ok);

                        NuntiumDiagnostics.SagaStateSaves.Add(1);
                    }
                    catch (Exception ex)
                    {
                        activity?.SetStatus(ActivityStatusCode.Error);

                        activity?.AddException(ex);

                        NuntiumDiagnostics.SagaFailures.Add(1);

                        throw;
                    }
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