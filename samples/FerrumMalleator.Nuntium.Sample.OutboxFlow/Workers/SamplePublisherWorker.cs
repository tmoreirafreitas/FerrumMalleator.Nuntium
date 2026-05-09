using FerrumMalleator.Nuntium.Abstractions.Publishers;
using FerrumMalleator.Nuntium.Sample.OutboxFlow.Messages;
using Microsoft.Extensions.Hosting;

namespace FerrumMalleator.Nuntium.Sample.OutboxFlow.Workers
{
    internal sealed class SamplePublisherWorker(IMessagePublisher publisher) : BackgroundService
    {
        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            Console.WriteLine("Publishing messages...");
            Console.WriteLine("Press CTRL+C to stop.");

            while (!stoppingToken.IsCancellationRequested)
            {
                var pedidoId = Guid.NewGuid();

                await publisher.PublishAsync(new PedidoCriado(pedidoId), stoppingToken);

                Console.WriteLine($"Pedido publicado: {pedidoId}");

                await Task.Delay(2000, stoppingToken);
            }
        }
    }
}
