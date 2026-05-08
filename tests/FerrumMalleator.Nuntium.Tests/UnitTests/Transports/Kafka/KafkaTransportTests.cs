using Confluent.Kafka;
using FerrumMalleator.Nuntium.Configuration;
using FerrumMalleator.Nuntium.Transport.Kafka;
using FluentAssertions;
using Microsoft.Extensions.Options;
using Moq;

namespace FerrumMalleator.Nuntium.Tests.UnitTests.Transports.Kafka
{
    public class KafkaTransportTests
    {
        [Fact]
        public async Task Should_send_message_to_kafka()
        {
            var producer = new Mock<IProducer<string, string>>();

            producer
                .Setup(x => x.ProduceAsync(
                    It.IsAny<string>(),
                    It.IsAny<Message<string, string>>(),
                    It.IsAny<CancellationToken>()))
                .ReturnsAsync(
                    new DeliveryResult<string, string>());

            var transport = new KafkaTransport(producer.Object);

            await transport.SendAsync("test-topic", "{}", CancellationToken.None);

            producer.Verify(x =>
                x.ProduceAsync(
                    "test-topic",
                    It.Is<Message<string, string>>(m =>
                        m.Value == "{}"),
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

            var transport = new KafkaTransport(producer.Object);

            var act = () => transport.SendAsync("test-topic", "{}", CancellationToken.None);

            await act.Should()
                .ThrowAsync<Exception>()
                .WithMessage("kafka error");
        }

        [Fact]
        public void Should_dispose_producer()
        {
            var producer = new Mock<IProducer<string, string>>();

            var transport = new KafkaTransport(producer.Object);

            transport.Dispose();

            producer.Verify(x => x.Flush(It.IsAny<TimeSpan>()), Times.Once);

            producer.Verify(x => x.Dispose(), Times.Once);
        }

        [Fact]
        public void Should_ignore_multiple_dispose()
        {
            var producer = new Mock<IProducer<string, string>>();

            var transport = new KafkaTransport(producer.Object);

            transport.Dispose();

            transport.Dispose();

            producer.Verify(x => x.Flush(It.IsAny<TimeSpan>()), Times.Once);

            producer.Verify(x => x.Dispose(), Times.Once);
        }

        [Fact]
        public void Should_throw_when_producer_creation_fails()
        {
            var factory = new Mock<IKafkaProducerFactory>();

            factory
                .Setup(x => x.Create(It.IsAny<ProducerConfig>()))
                .Throws(new Exception("fail"));

            var options = Options.Create(
                new KafkaOptions
                {
                    BootstrapServers = "localhost:9092"
                });

            var act = () => new KafkaTransport(options, factory.Object);

            act.Should()
                .Throw<InvalidOperationException>()
                .WithMessage("*Failed to initialize Kafka producer*");
        }
    }
}
