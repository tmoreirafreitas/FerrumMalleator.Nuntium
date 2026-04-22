using FerrumMalleator.Nuntium.Messaging.Models;

namespace FerrumMalleator.Nuntium.Abstractions.Persistence
{
    public interface IDeadLetterStore
    {
        Task AddAsync(DeadLetterMessage message, CancellationToken cancellation);
        Task<IReadOnlyList<DeadLetterMessage>> GetPendingAsync(int take, CancellationToken cancellation);
        Task MarkReprocessedAsync(Guid messageId, CancellationToken cancellation);
    }
}
