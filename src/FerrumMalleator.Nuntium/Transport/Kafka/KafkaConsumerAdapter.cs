using Confluent.Kafka;
using FerrumMalleator.Nuntium.Configuration;
using Microsoft.Extensions.Options;

namespace FerrumMalleator.Nuntium.Transport.Kafka
{
    internal sealed class KafkaConsumerAdapter : IKafkaConsumer, IDisposable
    {
        private readonly KafkaOptions _options;
        private readonly IConsumer<string, string> _inner;
        public KafkaConsumerAdapter(IOptions<KafkaOptions> options, string groupId)
        {
            if (options.Value == null)
                throw new ArgumentNullException(nameof(options.Value));

            _options = options.Value;

            var consumerConfig = new ConsumerConfig
            {
                BootstrapServers = _options.BootstrapServers,
                GroupId = groupId,
            };

            _options.ConsumerConfigAction?.Invoke(consumerConfig);

            _inner = new ConsumerBuilder<string, string>(consumerConfig).Build();
        }

        public ConsumeResult<string, string> Consume(CancellationToken ct) => _inner.Consume(ct);
        public void Commit(ConsumeResult<string, string> result) => _inner.Commit(result);
        public void Subscribe(IEnumerable<string> topics) => _inner.Subscribe(topics);
        public void Dispose() => _inner.Dispose();
    }
}
