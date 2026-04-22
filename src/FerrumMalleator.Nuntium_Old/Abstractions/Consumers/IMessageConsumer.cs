using System.Threading;
using System.Threading.Tasks;

namespace FerrumMalleator.Nuntium.Abstractions.Consumers
{
    public interface IMessageConsumer<T>
    {
        Task ConsumeAsync(T message, CancellationToken cancellation);
    }
}
