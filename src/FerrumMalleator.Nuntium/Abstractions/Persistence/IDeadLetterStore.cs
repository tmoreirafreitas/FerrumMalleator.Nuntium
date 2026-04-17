using FerrumMalleator.Nuntium.Messaging.Models;

namespace FerrumMalleator.Nuntium.Abstractions.Persistence
{
    public interface IDeadLetterStore
    {
        Task AddAsync(DeadLetterMessage message, CancellationToken ct);
        Task<IReadOnlyList<DeadLetterMessage>> GetPendingAsync(int take, CancellationToken ct);
        Task MarkReprocessedAsync(Guid messageId, CancellationToken ct);
    }
}
