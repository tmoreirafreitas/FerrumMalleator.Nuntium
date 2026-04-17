using FerrumMalleator.Nuntium.Abstractions;
using FerrumMalleator.Nuntium.Abstractions.Persistence;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace FerrumMalleator.Nuntium.Persistence.InMemory
{
    internal sealed class InMemorySagaRepository<T> : ISagaRepository<T> where T : class, ISagaState, new()
    {
        private readonly Dictionary<Guid, T> _store = new Dictionary<Guid, T>();

        public Task<T> GetAsync(Guid correlationId, CancellationToken cancellationToken = default)
        {
            _store.TryGetValue(correlationId, out var state);
            return Task.FromResult(state);
        }

        public Task SaveAsync(T state, CancellationToken cancellationToken = default)
        {
            _store[state.CorrelationId] = state;
            return Task.CompletedTask;
        }
    }
}
