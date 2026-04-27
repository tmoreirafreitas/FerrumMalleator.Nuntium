using FerrumMalleator.Nuntium.Abstractions.Persistence;
using FerrumMalleator.Nuntium.Abstractions.Publishers;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace FerrumMalleator.Nuntium.DeadLetter
{
    internal sealed class DeadLetterReprocessor(IServiceProvider provider) : BackgroundService
    {
        private readonly IServiceProvider _provider = provider;

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                await ProcessOnceAsync(stoppingToken);
                await Task.Delay(5000, stoppingToken);
            }
        }

        public async Task ProcessOnceAsync(CancellationToken stoppingToken)
        {
            using var scope = _provider.CreateScope();

            var store = scope.ServiceProvider.GetRequiredService<IDeadLetterStore>();
            var transport = scope.ServiceProvider.GetRequiredService<IMessageTransport>();

            var messages = await store.GetPendingAsync(50, stoppingToken);

            foreach (var msg in messages)
            {
                if (msg.NextRetryAt.HasValue && msg.NextRetryAt > DateTime.UtcNow)
                    continue;

                try
                {
                    await transport.SendAsync(
                        msg.MessageType,
                        msg.PayloadJson,
                        stoppingToken);

                    await store.MarkReprocessedAsync(msg.MessageId, stoppingToken);
                }
                catch
                {
                    msg.ReprocessCount++;

                    if (msg.ReprocessCount >= 5)
                    {
                        await store.MarkReprocessedAsync(msg.MessageId, stoppingToken);
                        continue;
                    }

                    var delay = TimeSpan.FromSeconds(Math.Pow(2, msg.ReprocessCount));

                    msg.NextRetryAt = DateTime.UtcNow.Add(delay);
                }
            }
        }
    }
}
