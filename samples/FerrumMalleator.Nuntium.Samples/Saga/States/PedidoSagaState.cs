using FerrumMalleator.Nuntium.Abstractions;

namespace FerrumMalleator.Nuntium.Samples.Saga.States
{
    public sealed class PedidoSagaState : ISagaState
    {
        public Guid CorrelationId { get; set; } = default!;
        public bool PedidoCriado { get; set; }
        public bool PagamentoAprovado { get; set; }
    }
}
