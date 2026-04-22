namespace FerrumMalleator.Nuntium.Abstractions.Publishers
{
    public interface IMessageTransport
    {
        Task SendAsync(string messageType, string payload, CancellationToken ct);
    }
}
