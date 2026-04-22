using FerrumMalleator.Nuntium.Abstractions.Persistence;

namespace FerrumMalleator.Nuntium.Persistence.InMemory
{
    internal sealed class InMemoryIdempotencyStore : IIdempotencyStore
    {
        private readonly HashSet<Guid> _processed = new HashSet<Guid>();

        public Task<bool> HasProcessedAsync(Guid messageId, CancellationToken cancellationToken = default)
        {
            return Task.FromResult(_processed.Contains(messageId));
        }

        public Task MarkProcessedAsync(Guid messageId, CancellationToken cancellationToken = default)
        {
            _processed.Add(messageId);
            return Task.CompletedTask;
        }
    }
}
