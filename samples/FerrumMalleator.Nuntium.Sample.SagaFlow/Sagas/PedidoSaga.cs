using FerrumMalleator.Nuntium.Abstractions.Publishers;
using FerrumMalleator.Nuntium.Sagas.Handlers;
using FerrumMalleator.Nuntium.Sample.SagaFlow.Messages;

namespace FerrumMalleator.Nuntium.Sample.SagaFlow.Sagas
{
    internal sealed class PedidoSaga(IMessagePublisher publisher) :
        ISagaHandler<PedidoCriado, PedidoSagaState>,
        ISagaHandler<PagamentoAprovado, PedidoSagaState>,
        ISagaHandler<EstoqueFinalizado, PedidoSagaState>
    {
        private readonly IMessagePublisher _publisher = publisher;

        public async Task HandleAsync(PedidoCriado message, PedidoSagaState state, CancellationToken stoppingToken)
        {
            state.PedidoCriado = true;

            Console.WriteLine($"[SAGA] Pedido criado | CorrelationId = {message.PedidoId}");

            await _publisher.PublishAsync(new AprovarPagamento(message.PedidoId), stoppingToken);
        }

        public async Task HandleAsync(PagamentoAprovado message, PedidoSagaState state, CancellationToken stoppingToken)
        {
            state.PagamentoAprovado = true;

            Console.WriteLine($"[SAGA] Pagamento aprovado | CorrelationId = {message.PedidoId}");

            await _publisher.PublishAsync(new SepararEstoque(message.PedidoId), stoppingToken);
        }

        public Task HandleAsync(EstoqueFinalizado message, PedidoSagaState state, CancellationToken stoppingToken)
        {
            state.Finalizado = true;

            Console.WriteLine($"[SAGA] Pedido finalizado | CorrelationId = {message.PedidoId}");

            return Task.CompletedTask;
        }
    }
}