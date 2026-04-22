using FerrumMalleator.Nuntium.Abstractions.Consumers;
using FerrumMalleator.Nuntium.Samples.Messages;

namespace FerrumMalleator.Nuntium.Samples.Consumers
{
    internal sealed class PedidoConsumer : IMessageConsumer<Pedido>
    {
        public Task ConsumeAsync(Pedido message, CancellationToken cancellation)
        {
            Console.WriteLine($"[CONSUMED] Pedido: {message.PedidoId}");

            return Task.CompletedTask;
        }
    }
}
