namespace FerrumMalleator.Nuntium.Abstractions.Publishers
{
    public interface IMessagePublisher
    {
        Task PublishAsync<T>(T message, CancellationToken cancellationToken = default);        
    }
}
