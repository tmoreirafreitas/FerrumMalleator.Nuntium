using Confluent.Kafka;

namespace FerrumMalleator.Nuntium.Transport.Kafka
{
    internal sealed class KafkaProducerFactory : IKafkaProducerFactory
    {
        public IProducer<string, string> Create(ProducerConfig config) => new ProducerBuilder<string, string>(config).Build();
    }
}
