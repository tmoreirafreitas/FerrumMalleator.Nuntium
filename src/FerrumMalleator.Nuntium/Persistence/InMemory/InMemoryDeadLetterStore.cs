using FerrumMalleator.Nuntium.Abstractions.Persistence;
using FerrumMalleator.Nuntium.Messaging.Models;

namespace FerrumMalleator.Nuntium.Persistence.InMemory
{
    internal sealed class InMemoryDeadLetterStore : IDeadLetterStore
    {
        private readonly List<DeadLetterMessage> _messages = [];

        public Task AddAsync(DeadLetterMessage message, CancellationToken ct)
        {
            _messages.Add(message);
            return Task.CompletedTask;
        }

        public async Task<IReadOnlyList<DeadLetterMessage>> GetPendingAsync(int take, CancellationToken ct)
        {
            var result = _messages
                .Where(x => !x.Reprocessed)
                .Take(take);

            return await Task.FromResult(result.ToList());
        }

        public Task MarkReprocessedAsync(Guid messageId, CancellationToken ct)
        {
            var msg = _messages.FirstOrDefault(x => x.MessageId == messageId);
            if (msg != null)
            {
                msg.Reprocessed = true;
            }

            return Task.CompletedTask;
        }
    }
}
