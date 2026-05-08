using FerrumMalleator.Nuntium.Abstractions;
using FerrumMalleator.Nuntium.Abstractions.Publishers;
using FerrumMalleator.Nuntium.Builders;
using FerrumMalleator.Nuntium.Transport.Kafka;
using FluentAssertions;
using Moq;

namespace FerrumMalleator.Nuntium.Tests.UnitTests.Transports.Kafka
{
    public class KafkaPublisherTests
    {
        private record TestMessage(Guid Id);

        [Fact]
        public async Task Should_publish_message_using_transport()
        {
            var transport = new Mock<IMessageTransport>();

            var resolver = new Mock<ITopicResolver>();

            resolver
                .Setup(x => x.Resolve<TestMessage>())
                .Returns("test-topic");

            var registry = new MessageMetadataRegistry();

            registry.Register<TestMessage>("test-key", "group");

            var publisher = new KafkaPublisher(transport.Object, resolver.Object, registry);

            var message = new TestMessage(Guid.NewGuid());
            var metadata = registry.Get<TestMessage>();
            await publisher.PublishAsync(message);

            transport.Verify(x =>
                x.SendAsync(
                    "test-topic",
                    It.Is<string>(payload =>
                        payload.Contains(metadata.Key)),
                    It.IsAny<CancellationToken>()),
                Times.Once);
        }

        [Fact]
        public async Task Should_create_valid_envelope()
        {
            var transport = new Mock<IMessageTransport>();

            var resolver = new Mock<ITopicResolver>();

            resolver
                .Setup(x => x.Resolve<TestMessage>())
                .Returns("test-topic");

            var registry = new MessageMetadataRegistry();

            registry.Register<TestMessage>("test-key", "group");

            var publisher = new KafkaPublisher(transport.Object, resolver.Object, registry);

            var message = new TestMessage(Guid.NewGuid());

            await publisher.PublishAsync(message);

            transport.Verify(x =>
                x.SendAsync(
                    "test-topic",
                    It.IsAny<string>(),
                    It.IsAny<CancellationToken>()),
                Times.Once);
        }

        [Fact]
        public async Task Should_throw_when_transport_fails()
        {
            var transport = new Mock<IMessageTransport>();

            transport
                .Setup(x => x.SendAsync(
                    It.IsAny<string>(),
                    It.IsAny<string>(),
                    It.IsAny<CancellationToken>()))
                .ThrowsAsync(new Exception("transport failed"));

            var resolver = new Mock<ITopicResolver>();

            resolver
                .Setup(x => x.Resolve<TestMessage>())
                .Returns("test-topic");

            var registry = new MessageMetadataRegistry();

            registry.Register<TestMessage>("test-key", "group");

            var publisher = new KafkaPublisher(transport.Object, resolver.Object, registry);

            var act = () => publisher.PublishAsync(new TestMessage(Guid.NewGuid()));

            await act.Should()
                .ThrowAsync<Exception>()
                .WithMessage("transport failed");
        }

        [Fact]
        public async Task Should_resolve_topic_before_sending()
        {
            var transport = new Mock<IMessageTransport>();

            var resolver = new Mock<ITopicResolver>();

            resolver
                .Setup(x => x.Resolve<TestMessage>())
                .Returns("pedido-topic");

            var registry = new MessageMetadataRegistry();

            registry.Register<TestMessage>("pedido-key", "group");

            var publisher = new KafkaPublisher(transport.Object, resolver.Object, registry);

            await publisher.PublishAsync(new TestMessage(Guid.NewGuid()));

            resolver.Verify(x => x.Resolve<TestMessage>(), Times.Once);

            transport.Verify(x =>
                x.SendAsync(
                    "pedido-topic",
                    It.IsAny<string>(),
                    It.IsAny<CancellationToken>()),
                Times.Once);
        }
    }
}