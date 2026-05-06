using FerrumMalleator.Nuntium.Outbox;

namespace FerrumMalleator.Nuntium.Abstractions.Persistence
{
    /// <summary>
    /// Persists Outbox messages for reliable delivery.
    /// </summary>
    /// <remarks>
    /// Used by the Outbox pattern to guarantee message consistency
    /// before dispatching to the transport.
    /// </remarks>
    public interface IOutboxStore
    {
        /// <summary>
        /// Adds a message to the Outbox.
        /// </summary>
        Task AddAsync(OutboxMessage message, CancellationToken cancellation);

        /// <summary>
        /// Retrieves pending Outbox messages.
        /// </summary>
        Task<IReadOnlyList<OutboxMessage>> GetPendingAsync(int take, CancellationToken cancellation);

        /// <summary>
        /// Marks a message as processed.
        /// </summary>
        Task MarkProcessedAsync(Guid id, CancellationToken cancellation);

        /// <summary>
        /// Marks a message as failed.
        /// </summary>
        Task MarkFailedAsync(Guid id, string error, CancellationToken cancellation);
    }
}
