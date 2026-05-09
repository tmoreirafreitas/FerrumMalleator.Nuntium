using FerrumMalleator.Nuntium.Abstractions.Consumers;
using FerrumMalleator.Nuntium.Abstractions.Publishers;
using FerrumMalleator.Nuntium.Sample.SagaFlow.Messages;

namespace FerrumMalleator.Nuntium.Sample.SagaFlow.Consumers
{
    internal sealed class PedidoConsumer(IMessagePublisher publisher) : IMessageConsumer<AprovarPagamento>, IMessageConsumer<SepararEstoque>
    {
        private readonly IMessagePublisher _publisher = publisher;

        public async Task ConsumeAsync(AprovarPagamento message, CancellationToken stoppingToken)
        {
            Console.WriteLine($"[CONSUMER] Recebido (Pedido: {message.PedidoId})");

            await _publisher.PublishAsync(new PagamentoAprovado(message.PedidoId), stoppingToken);
        }

        public async Task ConsumeAsync(SepararEstoque message, CancellationToken stoppingToken)
        {
            Console.WriteLine($"[CONSUMER] Pedido separado no estoque (Pedido: {message.PedidoId})");

            await _publisher.PublishAsync(new EstoqueFinalizado(message.PedidoId), stoppingToken);
        }
    }
}
