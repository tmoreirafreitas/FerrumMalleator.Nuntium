namespace FerrumMalleator.Nuntium.Abstractions.Persistence
{
    public interface IIdempotencyStore
    {
        Task<bool> HasProcessedAsync(Guid messageId, CancellationToken ct = default);
        Task MarkProcessedAsync(Guid messageId, CancellationToken ct = default);
    }
}
