using System;
using System.Threading;
using System.Threading.Tasks;

namespace FerrumMalleator.Nuntium.Sagas.Handlers
{
    public class SagaHandlerDescriptor
    {
        public Type MessageType { get; set; }
        public Type StateType { get; set; }
        public Func<object, object, IServiceProvider, CancellationToken, Task> Invoker { get; set; }
        public Func<IServiceProvider, string, CancellationToken, Task<object>> LoadState { get; set; }
        public Func<IServiceProvider, object, CancellationToken, Task> SaveState { get; set; }
    }
}
