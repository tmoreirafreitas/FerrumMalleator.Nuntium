using Confluent.Kafka;
using FerrumMalleator.Nuntium.Abstractions.Publishers;
using FerrumMalleator.Nuntium.Configuration;
using FerrumMalleator.Nuntium.Diagnostics;
using Microsoft.Extensions.Options;
using System.Diagnostics;

namespace FerrumMalleator.Nuntium.Transport.Kafka
{
    internal sealed class KafkaTransport : IMessageTransport, IDisposable
    {
        private readonly IProducer<string, string> _producer;

        private bool disposedValue;

        internal KafkaTransport(IProducer<string, string> producer)
        {
            _producer = producer;
        }

        public KafkaTransport(IOptions<KafkaOptions> options, IKafkaProducerFactory factory)
        {
            var producerConfig = new ProducerConfig
            {
                BootstrapServers = options.Value.BootstrapServers,
            };

            options.Value.ProducerConfigAction?.Invoke(producerConfig);

            try
            {
                _producer = factory.Create(producerConfig);
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException(
                """
                Failed to initialize Kafka producer.

                Possible causes:
                - Native libraries blocked by Windows Security (SmartScreen)
                - Missing OpenSSL dependencies
                - Platform mismatch

                Try:
                - Unblock DLLs (PowerShell: Unblock-File)
                - Run as administrator
                - Ensure x64 runtime
                """, ex);
            }
        }

        public async Task SendAsync(string topic, string payload, CancellationToken ct)
        {
            using var activity = NuntiumDiagnostics.ActivitySource.StartActivity("nuntium.kafka.send", ActivityKind.Producer);

            var start = Stopwatch.GetTimestamp();

            activity?.SetTag("messaging.system", "kafka");

            activity?.SetTag("messaging.operation", "send");

            activity?.SetTag("messaging.destination.name", topic);

            try
            {
                var message = new Message<string, string>
                {
                    Key = Guid.NewGuid().ToString(),
                    Value = payload
                };

                activity?.AddEvent(new ActivityEvent("kafka.message.producing"));

                await _producer
                    .ProduceAsync(topic, message, ct)
                    .ConfigureAwait(false);

                activity?.AddEvent(new ActivityEvent("kafka.message.published"));

                activity?.SetStatus(ActivityStatusCode.Ok);

                NuntiumDiagnostics.KafkaMessagesPublished.Add(1);
            }
            catch (Exception ex)
            {
                activity?.SetStatus(ActivityStatusCode.Error);

                activity?.AddException(ex);

                NuntiumDiagnostics.KafkaPublishFailures.Add(1);

                NuntiumDiagnostics.MessagesFailed.Add(1);

                throw;
            }
            finally
            {
                var elapsed = Stopwatch.GetElapsedTime(start);

                NuntiumDiagnostics.KafkaPublishDuration.Record(elapsed.TotalMilliseconds);
            }
        }

        private void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    _producer?.Flush(TimeSpan.FromSeconds(5));

                    _producer?.Dispose();
                }

                disposedValue = true;
            }
        }

        public void Dispose()
        {
            Dispose(disposing: true);

            GC.SuppressFinalize(this);
        }
    }
}