using System;
using System.Threading;
using System.Threading.Tasks;

namespace FerrumMalleator.Nuntium.Abstractions.Persistence
{
    public interface ISagaRepository<TState> where TState : class, ISagaState
    {
        Task<TState> GetAsync(Guid correlationId, CancellationToken cancellationToken = default);
        Task SaveAsync(TState state, CancellationToken cancellationToken = default);
    }
}
