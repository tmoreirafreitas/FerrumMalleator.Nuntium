using Confluent.Kafka;
using FerrumMalleator.Nuntium.Configuration;
using Microsoft.Extensions.Options;
using System.Diagnostics.CodeAnalysis;

namespace FerrumMalleator.Nuntium.Transport.Kafka
{
    [ExcludeFromCodeCoverage]
    internal sealed class KafkaConsumerAdapter : IKafkaConsumer, IDisposable
    {
        private readonly IConsumer<string, string> _inner;
        public KafkaConsumerAdapter(IOptions<KafkaOptions> options, string groupId)
        {
            if (options.Value == null)
                throw new ArgumentNullException(nameof(options));

            var localOptions = options.Value;

            var consumerConfig = new ConsumerConfig
            {
                BootstrapServers = localOptions.BootstrapServers,
                GroupId = groupId,
            };

            localOptions.ConsumerConfigAction?.Invoke(consumerConfig);

            _inner = new ConsumerBuilder<string, string>(consumerConfig).Build();
        }

        public ConsumeResult<string, string> Consume(CancellationToken ct) => _inner.Consume(ct);
        public void Commit(ConsumeResult<string, string> result) => _inner.Commit(result);
        public void Subscribe(IEnumerable<string> topics) => _inner.Subscribe(topics);
        public void Dispose() => _inner.Dispose();
    }
}
