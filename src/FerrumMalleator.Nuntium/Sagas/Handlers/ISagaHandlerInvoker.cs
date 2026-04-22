namespace FerrumMalleator.Nuntium.Sagas.Handlers
{
    public interface ISagaHandlerInvoker
    {
        Task Invoke(object message, object state, IServiceProvider provider, CancellationToken cancellationToken);
    }
}
