namespace FerrumMalleator.Nuntium.Abstractions.Consumers
{
    public interface IMessageConsumer<in T>
    {
        Task ConsumeAsync(T message, CancellationToken ct);
    }
}
