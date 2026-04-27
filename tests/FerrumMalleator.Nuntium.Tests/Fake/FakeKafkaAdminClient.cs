using Confluent.Kafka;
using Confluent.Kafka.Admin;
using FerrumMalleator.Nuntium.Transport.Kafka;

namespace FerrumMalleator.Nuntium.Tests.Fake
{
    internal class FakeKafkaAdminClient : IKafkaAdminClient
    {
        public List<TopicSpecification> CreatedTopics = new();
        public bool ThrowAlreadyExists;
        public bool ThrowOtherError;

        public Task CreateTopicsAsync(IEnumerable<TopicSpecification> topics)
        {
            if (ThrowOtherError)
            {
                throw new CreateTopicsException(new List<CreateTopicReport>
            {
                new() { Error = new Error(ErrorCode.Unknown) }
            });
            }

            if (ThrowAlreadyExists)
            {
                throw new CreateTopicsException(new List<CreateTopicReport>
            {
                new() { Error = new Error(ErrorCode.TopicAlreadyExists) }
            });
            }

            CreatedTopics.AddRange(topics);
            return Task.CompletedTask;
        }
    }
}
