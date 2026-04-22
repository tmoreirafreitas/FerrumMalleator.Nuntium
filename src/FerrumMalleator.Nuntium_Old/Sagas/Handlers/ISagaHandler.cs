using FerrumMalleator.Nuntium.Abstractions;
using System.Threading;
using System.Threading.Tasks;

namespace FerrumMalleator.Nuntium.Sagas.Handlers
{
    public interface ISagaHandler<TMessage, TState> where TState : class, ISagaState
    {
        Task Handle(TMessage message, TState state, CancellationToken cancellation);
    }
}
