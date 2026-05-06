namespace FerrumMalleator.Nuntium.Sagas.Handlers
{
    internal interface ISagaHandlerInvoker
    {
        Task Invoke(object message, object state, IServiceProvider provider, CancellationToken ct);
    }
}
