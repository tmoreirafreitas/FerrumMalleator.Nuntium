namespace FerrumMalleator.Nuntium.Abstractions
{
    public interface ISagaCorrelation<in TMessage>
    {
        Guid GetCorrelationId(TMessage message);
    }
}
