using FerrumMalleator.Nuntium.Messaging.Models;

namespace FerrumMalleator.Nuntium.Abstractions.Persistence
{
    /// <summary>
    /// Persists failed messages for later inspection or reprocessing.
    /// </summary>
    /// <remarks>
    /// Used by the Dead Letter Queue (DLQ) mechanism.
    /// </remarks>
    public interface IDeadLetterStore
    {
        /// <summary>
        /// Stores a failed message.
        /// </summary>
        Task AddAsync(DeadLetterMessage message, CancellationToken cancellation);

        /// <summary>
        /// Retrieves pending dead letter messages.
        /// </summary>
        Task<IReadOnlyList<DeadLetterMessage>> GetPendingAsync(int take, CancellationToken cancellation);

        /// <summary>
        /// Marks a message as successfully reprocessed.
        /// </summary>
        Task MarkReprocessedAsync(Guid messageId, CancellationToken cancellation);

        /// <summary>
        /// Updates retry metadata for a failed message.
        /// </summary>
        Task UpdateRetryAsync(Guid messageId, int reprocessCount, DateTime nextRetryAt, CancellationToken cancellation);
    }
}
