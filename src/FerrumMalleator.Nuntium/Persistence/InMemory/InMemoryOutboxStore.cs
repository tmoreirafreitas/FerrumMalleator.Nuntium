using FerrumMalleator.Nuntium.Abstractions.Persistence;
using FerrumMalleator.Nuntium.Outbox;
using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;

namespace FerrumMalleator.Nuntium.Persistence.InMemory
{
    [ExcludeFromCodeCoverage]
    internal sealed class InMemoryOutboxStore : IOutboxStore
    {
        private readonly ConcurrentBag<OutboxMessage> _messages = [];
        public IReadOnlyDictionary<Guid, OutboxMessage> Messages => _messages.ToDictionary(x => x.Id);

        public Task AddAsync(OutboxMessage message, CancellationToken cancellation)
        {
            _messages.Add(message);
            return Task.CompletedTask;
        }

        public Task<IReadOnlyList<OutboxMessage>> GetPendingAsync(int take, CancellationToken cancellation)
        {
            var result = _messages
                    .Where(x => x.ProcessedOn == null)
                    .Take(take).ToList().AsReadOnly();

            return Task.FromResult((IReadOnlyList<OutboxMessage>)result);
        }

        public Task MarkProcessedAsync(Guid id, CancellationToken cancellation)
        {
            var msg = _messages.First(x => x.Id == id);
            msg.ProcessedOn = DateTime.UtcNow;
            return Task.CompletedTask;
        }

        public Task MarkFailedAsync(Guid id, string error, CancellationToken cancellation)
        {
            var msg = _messages.First(x => x.Id == id);
            msg.Error = error;
            return Task.CompletedTask;
        }
    }
}
