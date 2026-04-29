using Confluent.Kafka;
using System.Diagnostics.CodeAnalysis;

namespace FerrumMalleator.Nuntium.Configuration
{
    [ExcludeFromCodeCoverage]
    public sealed class KafkaOptions
    {
        public string BootstrapServers { get; set; } = "localhost:9092";
        public int DefaultNumPartitions { get; set; } = 3;
        public short DefaultReplicationFactor { get; set; } = 1;
        internal Func<object, string>? PartitionKeyResolver { get; set; }
        internal Action<ProducerConfig>? ProducerConfigAction { get; set; }
        internal Action<ConsumerConfig>? ConsumerConfigAction { get; set; }

        public KafkaOptions ConfigureProducer(Action<ProducerConfig> configure)
        {
            ProducerConfigAction += configure;
            return this;
        }

        public KafkaOptions ConfigureConsumer(Action<ConsumerConfig> configure)
        {
            ConsumerConfigAction += configure;
            return this;
        }
    }
}
