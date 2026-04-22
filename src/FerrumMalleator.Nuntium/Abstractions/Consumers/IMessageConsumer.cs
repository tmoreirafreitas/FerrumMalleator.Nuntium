namespace FerrumMalleator.Nuntium.Abstractions.Consumers
{
    public interface IMessageConsumer<T>
    {
        Task ConsumeAsync(T message, CancellationToken stoppingToken);
    }
}
