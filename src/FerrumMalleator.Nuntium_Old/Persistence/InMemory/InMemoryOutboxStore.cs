using FerrumMalleator.Nuntium.Abstractions.Persistence;
using FerrumMalleator.Nuntium.Outbox;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace FerrumMalleator.Nuntium.Persistence.InMemory
{
    internal sealed class InMemoryOutboxStore : IOutboxStore
    {
        private readonly List<OutboxMessage> _messages = new List<OutboxMessage>();

        public Task AddAsync(OutboxMessage message, CancellationToken ct)
        {
            _messages.Add(message);
            return Task.CompletedTask;
        }

        public Task<IReadOnlyList<OutboxMessage>> GetPendingAsync(int take, CancellationToken ct)
        {
            var result = _messages
                    .Where(x => x.ProcessedOn == null)
                    .Take(take).ToList().AsReadOnly();

            return Task.FromResult((IReadOnlyList<OutboxMessage>)result);
        }

        public Task MarkProcessedAsync(Guid id, CancellationToken ct)
        {
            var msg = _messages.First(x => x.Id == id);
            msg.ProcessedOn = DateTime.UtcNow;
            return Task.CompletedTask;
        }

        public Task MarkFailedAsync(Guid id, string error, CancellationToken ct)
        {
            var msg = _messages.First(x => x.Id == id);
            msg.Error = error;
            return Task.CompletedTask;
        }
    }
}
