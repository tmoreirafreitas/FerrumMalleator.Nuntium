using Confluent.Kafka;
using FerrumMalleator.Nuntium.Dispatching;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using static Confluent.Kafka.ConfigPropertyNames;

namespace FerrumMalleator.Nuntium.Transport.Kafka
{
    internal sealed class KafkaConsumerWorker(IServiceProvider provider, string groupId, IEnumerable<string> topics) : BackgroundService
    {
        private readonly IServiceProvider _provider = provider;
        private readonly string _groupId = groupId;
        private readonly IEnumerable<string> _topics = topics;

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            using var externalScope = _provider.CreateScope();

            var loggerFactory = externalScope.ServiceProvider.GetRequiredService<ILoggerFactory>();
            var logger = loggerFactory.CreateLogger<KafkaConsumerWorker>();

            var consumer = externalScope.ServiceProvider.GetRequiredService<IKafkaConsumer>();
            consumer.Subscribe(_topics);

            while (!stoppingToken.IsCancellationRequested)
            {
                await ProcessOnceAsync(consumer, logger, stoppingToken);
            }
        }
        public async Task ProcessOnceAsync(IKafkaConsumer consumer, ILogger logger, CancellationToken stoppingToken)
        {
            try
            {
                var result = consumer?.Consume(stoppingToken);

                using var scope = _provider.CreateScope();
                var dispatcher = scope.ServiceProvider.GetRequiredService<MessageDispatcher>();

                logger?.LogDebug(@"Processing message {Topic} {Offset}", result?.Topic, result?.Offset);

                await dispatcher.DispatchAsync(result?.Message.Value!, stoppingToken);

                consumer?.Commit(result!);
            }
            catch (ConsumeException ex)
            {
                logger?.LogError(ex, "Kafka error ({GroupId}): {ErrorReason}", _groupId, ex.Error.Reason);
            }
            catch (OperationCanceledException)
            {
                // shutdown normal
            }
            catch (Exception ex)
            {
                logger?.LogError(ex, "Erro geral ({GroupId}): {Message}", _groupId, ex.Message);
            }
        }
    }
}
