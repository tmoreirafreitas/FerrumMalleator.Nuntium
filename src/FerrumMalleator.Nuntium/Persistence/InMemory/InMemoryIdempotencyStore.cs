using FerrumMalleator.Nuntium.Abstractions.Persistence;
using System.Diagnostics.CodeAnalysis;

namespace FerrumMalleator.Nuntium.Persistence.InMemory
{
    [ExcludeFromCodeCoverage]
    internal sealed class InMemoryIdempotencyStore : IIdempotencyStore
    {
        private readonly HashSet<Guid> _processed = [];

        public Task<bool> HasProcessedAsync(Guid messageId, CancellationToken cancellation = default)
        {
            return Task.FromResult(_processed.Contains(messageId));
        }

        public Task MarkProcessedAsync(Guid messageId, CancellationToken cancellation = default)
        {
            _processed.Add(messageId);
            return Task.CompletedTask;
        }
    }
}
