using Confluent.Kafka;
using FerrumMalleator.Nuntium.Abstractions;
using FerrumMalleator.Nuntium.Builders;
using FerrumMalleator.Nuntium.Configuration;
using FerrumMalleator.Nuntium.Messaging.Envelopes;
using FerrumMalleator.Nuntium.Transport.Kafka;
using FluentAssertions;
using Microsoft.Extensions.Options;
using Moq;
using System.Text.Json;

namespace FerrumMalleator.Nuntium.Tests.UnitTests.Transports.Kafka
{
    public class KafkaPublesherTests
    {
        private record TestMessage(Guid Id);

        [Fact]
        public async Task Should_publish_message_to_kafka()
        {
            var producer = new Mock<IProducer<string, string>>();

            producer
                .Setup(x => x.ProduceAsync(
                    It.IsAny<string>(),
                    It.IsAny<Message<string, string>>(),
                    It.IsAny<CancellationToken>()))
                .ReturnsAsync(new DeliveryResult<string, string>());

            var resolver = new Mock<ITopicResolver>();
            resolver.Setup(x => x.Resolve<TestMessage>())
                    .Returns("test-topic");

            var registry = new MessageMetadataRegistry();
            registry.Register<TestMessage>("test-key", "group");

            var publisher = new KafkaPublisher(producer.Object, resolver.Object, registry);

            var message = new TestMessage(Guid.NewGuid());

            await publisher.PublishAsync(message);

            producer.Verify(x => x.ProduceAsync(
                "test-topic",
                It.Is<Message<string, string>>(m => m.Value.Contains(registry.Get<TestMessage>().Key)),
                It.IsAny<CancellationToken>()),
                Times.Once);
        }

        [Fact]
        public async Task Should_send_raw_message()
        {
            var producer = new Mock<IProducer<string, string>>();

            producer
                .Setup(x => x.ProduceAsync(
                    It.IsAny<string>(),
                    It.IsAny<Message<string, string>>(),
                    It.IsAny<CancellationToken>()))
                .ReturnsAsync(new DeliveryResult<string, string>());

            var resolver = new Mock<ITopicResolver>();

            var registry = new MessageMetadataRegistry();
            registry.Register<TestMessage>("test-key", "group");

            var factory = new Mock<IKafkaProducerFactory>();
            var publisher = new KafkaPublisher(producer.Object, resolver.Object, registry);

            var metadata = registry.Get<TestMessage>();

            var envelope = new MessageEnvelope<TestMessage>
            {
                MessageId = Guid.NewGuid(),
                MessageType = metadata.Key,
                Payload = new TestMessage(Guid.NewGuid()),
            };

            var payload = JsonSerializer.Serialize(envelope);

            await publisher.SendAsync(metadata.Key, payload, CancellationToken.None);

            producer.Verify(x => x.ProduceAsync(
                It.IsAny<string>(),
                It.Is<Message<string, string>>(m => m.Value == payload),
                It.IsAny<CancellationToken>()),
                Times.Once);
        }

        [Fact]
        public async Task Should_throw_when_producer_fails()
        {
            var producer = new Mock<IProducer<string, string>>();

            producer
                .Setup(x => x.ProduceAsync(
                    It.IsAny<string>(),
                    It.IsAny<Message<string, string>>(),
                    It.IsAny<CancellationToken>()))
                .ThrowsAsync(new Exception("kafka error"));

            var resolver = new Mock<ITopicResolver>();
            resolver.Setup(x => x.Resolve<TestMessage>())
                    .Returns("test-topic");

            var registry = new MessageMetadataRegistry();
            registry.Register<TestMessage>("test-key", "group");

            var factory = new Mock<IKafkaProducerFactory>();
            var publisher = new KafkaPublisher(producer.Object, resolver.Object, registry);

            var act = () => publisher.PublishAsync(new TestMessage(Guid.NewGuid()));

            await act.Should().ThrowAsync<Exception>();
        }

        [Fact]
        public void Should_dispose_producer()
        {
            var producer = new Mock<IProducer<string, string>>();

            var publisher = new KafkaPublisher(producer.Object, Mock.Of<ITopicResolver>(), new MessageMetadataRegistry());

            publisher.Dispose();

            producer.Verify(x => x.Flush(It.IsAny<TimeSpan>()), Times.Once);
            producer.Verify(x => x.Dispose(), Times.Once);
        }

        [Fact]
        public void Should_throw_when_producer_creation_fails()
        {
            var factory = new Mock<IKafkaProducerFactory>();

            factory.Setup(x => x.Create(It.IsAny<ProducerConfig>()))
                   .Throws(new Exception("fail"));

            var options = Options.Create(new KafkaOptions
            {
                BootstrapServers = "localhost:9092"
            });

            var resolver = new Mock<ITopicResolver>();
            var registry = new MessageMetadataRegistry();

            var act = () => new KafkaPublisher(options, resolver.Object, registry, factory.Object);

            act.Should().Throw<InvalidOperationException>()
               .WithMessage("*Failed to initialize Kafka producer*");
        }

        [Fact]
        public async Task Should_use_custom_partition_key()
        {
            var producer = new Mock<IProducer<string, string>>();

            producer
                .Setup(x => x.ProduceAsync(
                    It.IsAny<string>(),
                    It.IsAny<Message<string, string>>(),
                    It.IsAny<CancellationToken>()))
                .ReturnsAsync(new DeliveryResult<string, string>());

            var resolver = new Mock<ITopicResolver>();

            resolver.Setup(x => x.Resolve<TestMessage>())
                .Returns("test-topic");

            var registry = new MessageMetadataRegistry();

            registry.Register<TestMessage>("test-key", "group", x => ((TestMessage)x).Id.ToString());

            var publisher = new KafkaPublisher(producer.Object, resolver.Object, registry);

            var message = new TestMessage(Guid.NewGuid());

            await publisher.PublishAsync(message);

            producer.Verify(x =>
                x.ProduceAsync(
                    "test-topic",
                    It.Is<Message<string, string>>(m =>
                        m.Key == message.Id.ToString()),
                    It.IsAny<CancellationToken>()),
                Times.Once);
        }

        [Fact]
        public async Task Should_throw_when_transport_send_fails()
        {
            var producer = new Mock<IProducer<string, string>>();

            producer
                .Setup(x => x.ProduceAsync(
                    It.IsAny<string>(),
                    It.IsAny<Message<string, string>>(),
                    It.IsAny<CancellationToken>()))
                .ThrowsAsync(new Exception("transport failed"));

            var resolver = new Mock<ITopicResolver>();

            var registry = new MessageMetadataRegistry();

            registry.Register<TestMessage>("test-key", "group");

            var publisher = new KafkaPublisher(producer.Object, resolver.Object, registry);

            var metadata = registry.Get<TestMessage>();

            var act = async () => await publisher.SendAsync(metadata.Key, "{}", CancellationToken.None);

            await act.Should().ThrowAsync<Exception>().WithMessage("transport failed");
        }

        [Fact]
        public async Task Should_throw_when_transport_metadata_not_found()
        {
            var producer = new Mock<IProducer<string, string>>();

            var resolver = new Mock<ITopicResolver>();

            var registry = new MessageMetadataRegistry();

            var publisher = new KafkaPublisher(producer.Object, resolver.Object, registry);

            var act = async () => await publisher.SendAsync("invalid-message", "{}", CancellationToken.None);

            await act.Should().ThrowAsync<KeyNotFoundException>();
        }

        [Fact]
        public void Should_ignore_multiple_dispose()
        {
            var producer = new Mock<IProducer<string, string>>();

            var publisher = new KafkaPublisher(producer.Object, Mock.Of<ITopicResolver>(), new MessageMetadataRegistry());

            publisher.Dispose();

            publisher.Dispose();

            producer.Verify(x => x.Flush(It.IsAny<TimeSpan>()), Times.Once);

            producer.Verify(x => x.Dispose(), Times.Once);
        }
    }
}
