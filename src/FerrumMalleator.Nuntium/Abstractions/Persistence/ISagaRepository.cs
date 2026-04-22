namespace FerrumMalleator.Nuntium.Abstractions.Persistence
{
    public interface ISagaRepository<TState> where TState : ISagaState
    {
        Task<TState> GetAsync(Guid correlationId, CancellationToken cancellation = default);
        Task SaveAsync(TState state, CancellationToken cancellation = default);
    }
}
