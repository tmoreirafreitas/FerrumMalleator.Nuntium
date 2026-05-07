using FerrumMalleator.Nuntium.Abstractions.Publishers;
using FerrumMalleator.Nuntium.Transport.Default;
using FluentAssertions;
using Moq;

namespace FerrumMalleator.Nuntium.Tests.UnitTests.Transports
{
    public class DefaultMessagePublisherTests
    {
        private record TestMessage(Guid Id);

        [Fact]
        public async Task Should_publish_message()
        {
            var transport = new Mock<IMessageTransport>();

            transport.Setup(x =>
                    x.SendAsync(
                        It.IsAny<string>(),
                        It.IsAny<string>(),
                        It.IsAny<CancellationToken>()))
                .Returns(Task.CompletedTask);

            var publisher = new DefaultMessagePublisher(transport.Object);

            var message = new TestMessage(Guid.NewGuid());

            await publisher.PublishAsync(message, CancellationToken.None);

            transport.Verify(x =>
                x.SendAsync(
                    nameof(TestMessage),
                    It.Is<string>(payload =>
                        payload.Contains(message.Id.ToString())),
                    It.IsAny<CancellationToken>()),
                Times.Once);
        }

        [Fact]
        public async Task Should_throw_when_transport_fails()
        {
            var transport = new Mock<IMessageTransport>();

            transport.Setup(x =>
                    x.SendAsync(
                        It.IsAny<string>(),
                        It.IsAny<string>(),
                        It.IsAny<CancellationToken>()))
                .ThrowsAsync(new Exception("transport failed"));

            var publisher = new DefaultMessagePublisher(transport.Object);

            var act = async () => await publisher.PublishAsync(new TestMessage(Guid.NewGuid()), CancellationToken.None);

            await act.Should().ThrowAsync<Exception>().WithMessage("transport failed");
        }
    }
}
