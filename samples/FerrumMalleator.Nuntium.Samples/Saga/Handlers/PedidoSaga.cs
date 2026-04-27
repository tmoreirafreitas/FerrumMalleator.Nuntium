using FerrumMalleator.Nuntium.Abstractions.Publishers;
using FerrumMalleator.Nuntium.Sagas.Handlers;
using FerrumMalleator.Nuntium.Samples.Messages;
using FerrumMalleator.Nuntium.Samples.Saga.States;

namespace FerrumMalleator.Nuntium.Samples.Saga.Handlers
{
    internal sealed class PedidoSaga(IMessagePublisher _publisher) : ISagaHandler<PedidoCriado, PedidoSagaState>, ISagaHandler<PagamentoAprovado, PedidoSagaState>
    {
        public async Task HandleAsync(PedidoCriado message, PedidoSagaState state, CancellationToken stoppingToken)
        {
            state.PedidoCriado = true;

            Console.WriteLine($"[SAGA] Pedido criado  | CorrelationId = {message.PedidoId}");

            await TryFinalize(state, message.PedidoId, stoppingToken);
        }

        public async Task HandleAsync(PagamentoAprovado message, PedidoSagaState state, CancellationToken stoppingToken)
        {
            state.PagamentoAprovado = true;

            Console.WriteLine($"[SAGA] Pagamento aprovado | CorrelationId = {message.PedidoId}");

            await TryFinalize(state, message.PedidoId, stoppingToken);
        }

        private async Task TryFinalize(PedidoSagaState state, Guid pedidoId, CancellationToken ct)
        {
            if (state.PedidoCriado && state.PagamentoAprovado)
            {
                Console.WriteLine($"[SAGA] Pedido finalizado | CorrelationId = {pedidoId}");

                await _publisher.PublishAsync(new PedidoFinalizado(pedidoId), ct);
            }
        }
    }
}
