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
        public bool CreateTopicsCalled;
        public bool ThrowGenericError;

        public Task CreateTopicsAsync(IEnumerable<TopicSpecification> topics)
        {
            CreateTopicsCalled = true;

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

            if (ThrowGenericError)
                throw new Exception("generic failure");

            CreatedTopics.AddRange(topics);
            return Task.CompletedTask;
        }
    }
}
