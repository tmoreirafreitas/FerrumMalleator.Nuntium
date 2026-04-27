using Confluent.Kafka;

namespace FerrumMalleator.Nuntium.Transport.Kafka
{
    public interface IKafkaConsumer
    {
        ConsumeResult<string, string> Consume(CancellationToken ct);
        void Commit(ConsumeResult<string, string> result);
        void Subscribe(IEnumerable<string> topics);
    }
}
