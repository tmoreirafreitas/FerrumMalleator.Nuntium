
using FerrumMalleator.Nuntium.Abstractions.Publishers;
using FerrumMalleator.Nuntium.Samples.Messages;

namespace FerrumMalleator.Nuntium.Samples
{
    public class SamplePublisherWorker(IServiceProvider provider) : BackgroundService
    {
        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            using var scope = provider.CreateScope();
            var publisher = scope.ServiceProvider.GetRequiredService<IMessagePublisher>();

            while (!stoppingToken.IsCancellationRequested)
            {
                await Task.Delay(1000, stoppingToken);

                var pedido = new PedidoCriado(Guid.NewGuid());

                await publisher.PublishAsync(pedido, stoppingToken);
            }
        }
    }
}
