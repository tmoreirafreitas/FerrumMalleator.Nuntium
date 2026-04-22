
using FerrumMalleator.Nuntium.Abstractions.Publishers;
using FerrumMalleator.Nuntium.Samples.Messages;

namespace FerrumMalleator.Nuntium.Samples
{
    public class SamplePublisherWorker(IMessagePublisher publisher) : BackgroundService
    {
        private readonly IMessagePublisher _publisher = publisher;

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            await Task.Delay(2000, stoppingToken);

            var pedido = new Pedido(Guid.NewGuid(), Guid.NewGuid(), Random.Shared.NextDouble());

            Console.WriteLine($"[PUBLISH] Pedido: {pedido.PedidoId}");

            await _publisher.PublishAsync(pedido, stoppingToken);
        }
    }
}
