using Confluent.Kafka.Admin;
using FerrumMalleator.Nuntium.Abstractions;
using FerrumMalleator.Nuntium.Abstractions.Persistence;
using FerrumMalleator.Nuntium.Abstractions.Publishers;
using FerrumMalleator.Nuntium.Builders;
using FerrumMalleator.Nuntium.Configuration;
using FerrumMalleator.Nuntium.Dispatching;
using FerrumMalleator.Nuntium.Messaging.Envelopes;
using FerrumMalleator.Nuntium.Messaging.Models;
using FerrumMalleator.Nuntium.Outbox;
using FerrumMalleator.Nuntium.Persistence.InMemory;
using FerrumMalleator.Nuntium.Retry;
using FerrumMalleator.Nuntium.Tests.Fake;
using FerrumMalleator.Nuntium.Transport.InMemory;
using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Moq;
using System.Text.Json;

namespace FerrumMalleator.Nuntium.Tests.UnitTests.Processors
{
    public class OutboxProcessorTests
    {
        private record TestMessage(Guid Id);
        internal class FailingRetryExecutor : IRetryExecutor
        {
            public Task ExecuteAsync(Func<Task> action, Func<Exception, int, Task> onFailure)
            {
                return onFailure(new Exception("forced failure"), 1);
            }
        }

        [Fact]
        public async Task Should_process_pending_messages()
        {
            var services = new ServiceCollection();

            var store = new InMemoryOutboxStore();
            var registry = new MessageMetadataRegistry();
            registry.Register<TestMessage>("test", "group");
            registry.Register<OutboxMessage>("outbox", "group");
            registry.Register<DeadLetterMessage>("dlq", "group");

            var consumerRegistry = new ConsumerInvokerRegistry();
            consumerRegistry.Register<OutboxMessage>();

            services.AddSingleton(registry);
            services.AddSingleton(consumerRegistry);

            var metadata = registry.Get<OutboxMessage>();
            var msg = new OutboxMessage
            {
                Id = Guid.NewGuid(),
                Type = metadata.Key,
                Payload = JsonSerializer.Serialize(new TestMessage(Guid.NewGuid()))
            };

            await store.AddAsync(msg, CancellationToken.None);

            var transporte = new FakeTransport { Sent = true };
            services.AddScoped<MessageDispatcher>();
            services.AddSingleton<IOutboxStore>(store);
            services.AddSingleton<IMessageTransport>(transporte);
            services.AddSingleton<IRetryExecutor>(new RetryExecutor(new RetryPolicyOptions
            {
                MaxAttempts = 1,
                Delays = []
            }));

            var provider = services.BuildServiceProvider();

            var processor = new OutboxProcessor(provider);

            // Act
            await processor.ProcessOnceAsync(CancellationToken.None);

            // Assert
            transporte.Sent.Should().BeTrue();

            var pendings = await store.GetPendingAsync(1, CancellationToken.None);
            pendings.Should().BeEmpty();
        }

        [Fact]
        public async Task Should_send_to_dlq_and_mark_failed_when_transport_fails()
        {
            var services = new ServiceCollection();

            var store = new InMemoryOutboxStore();
            var registry = new MessageMetadataRegistry();

            registry.Register<TestMessage>("test", "group");
            registry.Register<DeadLetterMessage>("dlq", "group");

            services.AddSingleton(registry);
            services.AddSingleton<IOutboxStore>(store);

            var transporte = new FakeTransport { Throw = true };

            services.AddSingleton<IMessageTransport>(transporte);

            services.AddSingleton<IRetryExecutor>(new RetryExecutor(new RetryPolicyOptions
            {
                MaxAttempts = 1,
                Delays = []
            }));

            var metadata = registry.Get<TestMessage>();

            var envelope = new MessageEnvelope<TestMessage>
            {
                MessageId = Guid.NewGuid(),
                MessageType = metadata.Key,
                Payload = new TestMessage(Guid.NewGuid())
            };

            var msg = new OutboxMessage
            {
                Id = Guid.NewGuid(),
                Type = metadata.Key,
                Payload = JsonSerializer.Serialize(envelope),
                OccurredOn = DateTime.UtcNow,
                Error = "fail",
            };

            await store.AddAsync(msg, CancellationToken.None);

            var provider = services.BuildServiceProvider();
            var processor = new OutboxProcessor(provider);

            // Act
            await Assert.ThrowsAsync<Exception>(() => processor.ProcessOnceAsync(CancellationToken.None));

            // Assert
            transporte.Throw.Should().BeTrue();
            store.Messages[msg.Id].Error.Should().NotBeNull();
        }

        [Fact]
        public async Task Should_mark_as_processed_when_success()
        {
            var store = new Mock<IOutboxStore>();
            var transport = new Mock<IMessageTransport>();

            var registry = new MessageMetadataRegistry();
            registry.Register<TestMessage>("test", "group");
            registry.Register<OutboxMessage>("outbox", "group");

            var envelope = new MessageEnvelope<TestMessage>
            {
                MessageId = Guid.NewGuid(),
                MessageType = registry.Get<TestMessage>().Key,
                Payload = new TestMessage(Guid.NewGuid())
            };

            var msg = new OutboxMessage
            {
                Id = Guid.NewGuid(),
                Type = registry.Get<OutboxMessage>().Key,
                Payload = JsonSerializer.Serialize(envelope)
            };

            store.Setup(x => x.GetPendingAsync(It.IsAny<int>(), It.IsAny<CancellationToken>()))
                 .ReturnsAsync(new List<OutboxMessage> { msg });

            transport.Setup(x => x.SendAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<CancellationToken>()))
                     .Returns(Task.CompletedTask);

            var services = new ServiceCollection();
            services.AddSingleton(store.Object);
            services.AddSingleton(transport.Object);
            services.AddSingleton(registry);
            services.AddSingleton<IRetryExecutor>(new RetryExecutor(new RetryPolicyOptions
            {
                MaxAttempts = 1,
                Delays = []
            }));

            var provider = services.BuildServiceProvider();

            var processor = new OutboxProcessor(provider);

            await processor.ProcessOnceAsync(CancellationToken.None);

            store.Verify(x => x.MarkProcessedAsync(msg.Id, It.IsAny<CancellationToken>()), Times.Once);
            store.Verify(x => x.MarkFailedAsync(It.IsAny<Guid>(), It.IsAny<string>(), It.IsAny<CancellationToken>()), Times.Never);
        }

        [Fact]
        public async Task Should_mark_as_failed_when_retry_fails()
        {
            var store = new Mock<IOutboxStore>();
            var transport = new Mock<IMessageTransport>();

            var registry = new MessageMetadataRegistry();
            registry.Register<TestMessage>("test", "group");
            registry.Register<DeadLetterMessage>("dlq", "group");
            registry.Register<OutboxMessage>("outbox", "group");

            var envelope = new MessageEnvelope<TestMessage>
            {
                MessageId = Guid.NewGuid(),
                MessageType = registry.Get<TestMessage>().Key,
                Payload = new TestMessage(Guid.NewGuid())
            };

            var msg = new OutboxMessage
            {
                Id = Guid.NewGuid(),
                Type = registry.Get<OutboxMessage>().Key,
                Payload = JsonSerializer.Serialize(envelope)
            };

            store.Setup(x => x.GetPendingAsync(It.IsAny<int>(), It.IsAny<CancellationToken>()))
                 .ReturnsAsync(new List<OutboxMessage> { msg });

            transport.Setup(x => x.SendAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<CancellationToken>()))
                     .Returns(Task.CompletedTask);

            var services = new ServiceCollection();
            services.AddSingleton(store.Object);
            services.AddSingleton(transport.Object);
            services.AddSingleton(registry);
            services.AddSingleton<IRetryExecutor, FailingRetryExecutor>();

            var provider = services.BuildServiceProvider();

            var processor = new OutboxProcessor(provider);

            await processor.ProcessOnceAsync(CancellationToken.None);

            store.Verify(x => x.MarkFailedAsync(msg.Id, It.IsAny<string>(), It.IsAny<CancellationToken>()), Times.Once);

            transport.Verify(x => x.SendAsync(
                It.IsAny<string>(),
                It.IsAny<string>(),
                It.IsAny<CancellationToken>()),
                Times.AtLeastOnce);
        }
    }
}
