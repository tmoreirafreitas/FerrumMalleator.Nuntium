using FerrumMalleator.Nuntium.Abstractions.Persistence;
using FerrumMalleator.Nuntium.Builders;
using FerrumMalleator.Nuntium.Outbox;
using FerrumMalleator.Nuntium.Persistence.InMemory;
using FluentAssertions;
using Moq;

namespace FerrumMalleator.Nuntium.Tests.UnitTests.Outbox
{
    public class OutboxPublisherTests
    {
        private record TestMessage(Guid Id);

        [Fact]
        public async Task Should_store_message_in_outbox()
        {
            var store = new InMemoryOutboxStore();

            var registry = new MessageMetadataRegistry();

            registry.Register<TestMessage>("test", "group");

            var publisher = new OutboxPublisher(store, registry);

            await publisher.PublishAsync(new TestMessage(Guid.NewGuid()), CancellationToken.None);

            var messages = await store.GetPendingAsync(10, CancellationToken.None);

            messages.Should().HaveCount(1);

            var message = messages.Single();

            message.Type.Should().Be("TestMessage");

            message.Payload.Should().NotBeNullOrWhiteSpace();
        }

        [Fact]
        public async Task Should_throw_when_message_not_registered()
        {
            var store = new InMemoryOutboxStore();

            var registry = new MessageMetadataRegistry();

            var publisher = new OutboxPublisher(store, registry);

            var act = async () => await publisher.PublishAsync(new TestMessage(Guid.NewGuid()), CancellationToken.None);

            await act.Should().ThrowAsync<KeyNotFoundException>();
        }

        [Fact]
        public async Task Should_throw_when_outbox_store_fails()
        {
            var store = new Mock<IOutboxStore>();

            store.Setup(x =>
                    x.AddAsync(
                        It.IsAny<OutboxMessage>(),
                        It.IsAny<CancellationToken>()))
                .ThrowsAsync(new Exception("store failed"));

            var registry = new MessageMetadataRegistry();

            registry.Register<TestMessage>("test", "group");

            var publisher = new OutboxPublisher(store.Object, registry);

            var act = async () =>
                await publisher.PublishAsync(new TestMessage(Guid.NewGuid()), CancellationToken.None);

            await act.Should()
                .ThrowAsync<Exception>()
                .WithMessage("store failed");
        }
    }
}
