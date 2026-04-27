using FerrumMalleator.Nuntium.Abstractions;

namespace FerrumMalleator.Nuntium.Sagas.Handlers
{
    public interface ISagaHandler<in TMessage, in TState> where TState : ISagaState
    {
        Task HandleAsync(TMessage message, TState state, CancellationToken stoppingToken);
    }
}
