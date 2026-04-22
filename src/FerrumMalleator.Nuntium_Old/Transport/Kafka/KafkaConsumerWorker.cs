using Confluent.Kafka;
using FerrumMalleator.Nuntium.Configuration;
using FerrumMalleator.Nuntium.Dispatching;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace FerrumMalleator.Nuntium.Transport.Kafka
{
    public class KafkaConsumerWorker : BackgroundService
    {
        private readonly IServiceProvider _provider;
        private readonly KafkaOptions _options;
        private readonly string _groupId;
        private readonly IEnumerable<string> _topics;

        public KafkaConsumerWorker(IServiceProvider provider,
        IOptions<KafkaOptions> options,
        string groupId,
        IEnumerable<string> topics)
        {
            _provider = provider;
            _options = options.Value ?? new KafkaOptions();
            _groupId = groupId;
            _topics = topics;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            var consumerConfig = new ConsumerConfig
            {
                BootstrapServers = _options.BootstrapServers,
                GroupId = _groupId,
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnableAutoCommit = false
            };

            using (var consumer = new ConsumerBuilder<string, string>(consumerConfig).Build())
            {
                consumer.Subscribe(_topics);

                using (var externalScope = _provider.CreateScope())
                {
                    var loggerFactory = externalScope.ServiceProvider.GetRequiredService<ILoggerFactory>();
                    var logger = loggerFactory.CreateLogger<KafkaConsumerWorker>();

                    while (!stoppingToken.IsCancellationRequested)
                    {
                        try
                        {
                            var result = consumer.Consume(stoppingToken);

                            using (var scope = _provider.CreateScope())
                            {
                                var dispatcher = scope.ServiceProvider.GetRequiredService<MessageDispatcher>();

                                await dispatcher.DispatchAsync(result.Message.Value, stoppingToken);

                                consumer.Commit(result);
                            }
                        }
                        catch (ConsumeException ex)
                        {
                            logger.LogError(ex, "Kafka error ({GroupId}): {ErrorReason}", _groupId, ex.Error.Reason);
                        }
                        catch (OperationCanceledException)
                        {
                            // shutdown normal
                        }
                        catch (Exception ex)
                        {
                            logger.LogError(ex, "Erro geral ({GroupId}): {Message}", _groupId, ex.Message);
                        }
                    }
                }
            }
        }
    }
}
