using FerrumMalleator.Nuntium.Abstractions.Consumers;
using FerrumMalleator.Nuntium.Sample.OutboxFlow.Messages;

namespace FerrumMalleator.Nuntium.Sample.OutboxFlow.Consumers
{
    internal sealed class PedidoConsumer : IMessageConsumer<PedidoCriado>
    {
        public Task ConsumeAsync(PedidoCriado message, CancellationToken ct)
        {
            Console.WriteLine($"[CONSUMER] Pedido recebido | PedidoId = {message.PedidoId}");

            return Task.CompletedTask;
        }
    }
}