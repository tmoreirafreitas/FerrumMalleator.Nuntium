using FerrumMalleator.Nuntium.Abstractions.Persistence;
using FerrumMalleator.Nuntium.Abstractions.Publishers;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace FerrumMalleator.Nuntium.Outbox
{
    public class OutboxProcessor : BackgroundService
    {
        private readonly IServiceProvider _provider;
        public OutboxProcessor(IServiceProvider provider)
        {
            _provider = provider;
        }        

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                using (var scope = _provider.CreateScope())
                {
                    var store = scope.ServiceProvider.GetRequiredService<IOutboxStore>();
                    var publisher = scope.ServiceProvider.GetRequiredService<IMessagePublisher>();

                    var messages = await store.GetPendingAsync(50, stoppingToken);

                    foreach (var msg in messages)
                    {
                        try
                        {
                            await publisher.PublishAsync(msg.Payload, stoppingToken);

                            await store.MarkProcessedAsync(msg.Id, stoppingToken);
                        }
                        catch (Exception ex)
                        {
                            await store.MarkFailedAsync(msg.Id, ex.Message, stoppingToken);
                        }
                    }

                    await Task.Delay(1000, stoppingToken);
                }

            }
        }
    }
}
