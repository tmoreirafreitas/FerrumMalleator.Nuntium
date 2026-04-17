namespace FerrumMalleator.Nuntium.Abstractions
{
    public interface ISagaCorrelation<TMessage>
    {
        Guid GetCorrelationId(TMessage message);
    }
}
