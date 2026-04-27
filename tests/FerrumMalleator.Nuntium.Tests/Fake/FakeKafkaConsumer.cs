using Confluent.Kafka;
using FerrumMalleator.Nuntium.Transport.Kafka;

namespace FerrumMalleator.Nuntium.Tests.Fake
{
    internal class FakeKafkaConsumer : IKafkaConsumer
    {
        public bool CommitCalled;
        public bool Throw;
        public ConsumeResult<string, string>? Result;

        public ConsumeResult<string, string> Consume(CancellationToken ct)
        {
            if (Throw)
                throw new ConsumeException(
                    new ConsumeResult<byte[], byte[]>(),
                    new Error(ErrorCode.Unknown));

            return Result!;
        }

        public void Commit(ConsumeResult<string, string> result)
        {
            CommitCalled = true;
        }

        public void Subscribe(IEnumerable<string> topics) { }
    }
}
