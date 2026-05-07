using Confluent.Kafka;
using FerrumMalleator.Nuntium.Diagnostics;
using FerrumMalleator.Nuntium.Dispatching;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System.Diagnostics;

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
            using var activity = NuntiumDiagnostics.ActivitySource.StartActivity("nuntium.kafka.consume", ActivityKind.Consumer);

            var start = Stopwatch.GetTimestamp();

            activity?.SetTag("messaging.system", "kafka");

            activity?.SetTag("messaging.operation", "receive");

            activity?.SetTag("messaging.kafka.consumer_group", _groupId);

            try
            {
                var result = consumer?.Consume(stoppingToken);

                if (result == null)
                    return;

                activity?.SetTag("messaging.destination.name", result.Topic);

                activity?.SetTag("messaging.kafka.partition", result.Partition.Value);

                activity?.SetTag("messaging.kafka.offset", result.Offset.Value);

                activity?.AddEvent(new ActivityEvent("kafka.message.received"));

                using var scope = _provider.CreateScope();

                var dispatcher = scope.ServiceProvider.GetRequiredService<MessageDispatcher>();

                logger?.LogDebug(@"Processing message {Topic} {Offset}", result.Topic, result.Offset);

                await dispatcher.DispatchAsync(result.Message.Value!, stoppingToken);

                consumer?.Commit(result);

                activity?.AddEvent(new ActivityEvent("kafka.message.committed"));

                activity?.SetStatus(ActivityStatusCode.Ok);

                NuntiumDiagnostics.KafkaMessagesConsumed.Add(1);
            }
            catch (ConsumeException ex)
            {
                activity?.SetStatus(ActivityStatusCode.Error);

                activity?.AddException(ex);

                NuntiumDiagnostics.KafkaConsumeFailures.Add(1);

                logger?.LogError(ex, "Kafka error ({GroupId}): {ErrorReason}", _groupId, ex.Error.Reason);
            }
            catch (OperationCanceledException)
            {
                activity?.AddEvent(new ActivityEvent("kafka.consumer.cancelled"));
            }
            catch (Exception ex)
            {
                activity?.SetStatus(ActivityStatusCode.Error);

                activity?.AddException(ex);

                NuntiumDiagnostics.KafkaCommitFailures.Add(1);

                logger?.LogError(ex, "Erro geral ({GroupId}): {Message}", _groupId, ex.Message);
            }
            finally
            {
                var elapsed = Stopwatch.GetElapsedTime(start);

                NuntiumDiagnostics.KafkaConsumeDuration.Record(elapsed.TotalMilliseconds);
            }
        }
    }
}