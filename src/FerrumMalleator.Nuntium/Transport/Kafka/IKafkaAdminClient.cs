using Confluent.Kafka.Admin;

namespace FerrumMalleator.Nuntium.Transport.Kafka
{
    public interface IKafkaAdminClient
    {
        Task CreateTopicsAsync(IEnumerable<TopicSpecification> topics);
    }
}
