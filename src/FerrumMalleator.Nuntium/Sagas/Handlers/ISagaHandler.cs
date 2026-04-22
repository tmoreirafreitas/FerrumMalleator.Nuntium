using FerrumMalleator.Nuntium.Abstractions;

namespace FerrumMalleator.Nuntium.Sagas.Handlers
{
    public interface ISagaHandler<TMessage, TState> where TState : class, ISagaState
    {
        Task Handle(TMessage message, TState state, CancellationToken stoppingToken);
    }
}
