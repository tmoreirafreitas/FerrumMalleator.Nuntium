using FerrumMalleator.Nuntium.Abstractions.Consumers;
using FerrumMalleator.Nuntium.Abstractions.Publishers;
using FerrumMalleator.Nuntium.Samples.Messages;

namespace FerrumMalleator.Nuntium.Samples.Consumers
{
    internal sealed class PedidoConsumer(IMessagePublisher publisher) : IMessageConsumer<PedidoCriado>
    {
        private readonly IMessagePublisher _publisher = publisher;

        public async Task ConsumeAsync(PedidoCriado message, CancellationToken stoppingToken)
        {
            Console.WriteLine($"[CONSUMER] Recebido (Pedido: {message.PedidoId})");

            await _publisher.PublishAsync(new PagamentoAprovado(message.PedidoId), stoppingToken);
        }
    }
}
