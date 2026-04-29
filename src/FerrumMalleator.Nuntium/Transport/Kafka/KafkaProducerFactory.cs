using Confluent.Kafka;
using System.Diagnostics.CodeAnalysis;

namespace FerrumMalleator.Nuntium.Transport.Kafka
{
    [ExcludeFromCodeCoverage]
    internal sealed class KafkaProducerFactory : IKafkaProducerFactory
    {
        public IProducer<string, string> Create(ProducerConfig config) => new ProducerBuilder<string, string>(config).Build();
    }
}
