namespace FerrumMalleator.Nuntium.Abstractions.Persistence
{
    public interface IIdempotencyStore
    {
        Task<bool> HasProcessedAsync(Guid messageId, CancellationToken cancellation = default);
        Task MarkProcessedAsync(Guid messageId, CancellationToken cancellation = default);
    }
}
