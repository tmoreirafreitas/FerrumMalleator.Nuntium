using FerrumMalleator.Nuntium.Abstractions.Publishers;
using FerrumMalleator.Nuntium.Samples.Messages;

namespace FerrumMalleator.Nuntium.Samples.OutBox
{
    public class OutBoxServiceWorker(IMessagePublisher publisher) : BackgroundService
    {
        private readonly IMessagePublisher _publisher = publisher;
        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                await Task.Delay(2000, stoppingToken);

                try
                {
                    var random = new Random();

                    double min = 10;
                    double max = 1000;

                    double valor = Math.Round(min + random.NextDouble() * (max - min), 2);

                    var pedido = new Pedido(Guid.NewGuid(), Guid.NewGuid(), valor);

                    Console.WriteLine($"[APP] Criando pedido {pedido.PedidoId}");

                    await _publisher.PublishAsync(pedido);

                    throw new Exception("Falha depois do publish");
                }
                catch (Exception)
                {
                }
            }
        }
    }
}
