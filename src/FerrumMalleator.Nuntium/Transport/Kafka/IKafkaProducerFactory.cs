using Confluent.Kafka;

namespace FerrumMalleator.Nuntium.Transport.Kafka
{
    public interface IKafkaProducerFactory
    {
        IProducer<string, string> Create(ProducerConfig config);
    }
}
