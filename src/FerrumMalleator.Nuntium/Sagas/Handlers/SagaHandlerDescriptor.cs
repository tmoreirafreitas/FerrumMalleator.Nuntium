namespace FerrumMalleator.Nuntium.Sagas.Handlers
{
    internal sealed class SagaHandlerDescriptor
    {
        public Type MessageType { get; set; } = default!;
        public Type StateType { get; set; } = default!;
        public Func<object, object, IServiceProvider, CancellationToken, Task> Invoker { get; set; } = default!;
        public Func<IServiceProvider, string, CancellationToken, Task<object>> LoadState { get; set; } = default!;
        public Func<IServiceProvider, object, CancellationToken, Task> SaveState { get; set; } = default!;
    }
}
