using FerrumMalleator.Nuntium.Abstractions.Consumers;
using Serilog;

namespace FerrumMalleator.Nuntium.Sample.BasicFlow
{
    public sealed class PedidoConsumer : IMessageConsumer<PedidoCriado>
    {
        public Task ConsumeAsync(PedidoCriado message, CancellationToken ct)
        {
            Log.Information("Pedido recebido: {PedidoId}", message.PedidoId);

            return Task.CompletedTask;
        }
    }
}
