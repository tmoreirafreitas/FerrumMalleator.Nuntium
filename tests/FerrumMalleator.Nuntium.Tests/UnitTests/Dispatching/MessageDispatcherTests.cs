using FerrumMalleator.Nuntium.Abstractions;
using FerrumMalleator.Nuntium.Abstractions.Consumers;
using FerrumMalleator.Nuntium.Abstractions.Persistence;
using FerrumMalleator.Nuntium.Abstractions.Publishers;
using FerrumMalleator.Nuntium.Builders;
using FerrumMalleator.Nuntium.Configuration;
using FerrumMalleator.Nuntium.Dispatching;
using FerrumMalleator.Nuntium.Messaging.Envelopes;
using FerrumMalleator.Nuntium.Messaging.Models;
using FerrumMalleator.Nuntium.Persistence.InMemory;
using FerrumMalleator.Nuntium.Retry;
using FerrumMalleator.Nuntium.Sagas.Handlers;
using FerrumMalleator.Nuntium.Tests.Fake;
using FerrumMalleator.Nuntium.Transport.InMemory;
using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Moq;
using System.Text.Json;

namespace FerrumMalleator.Nuntium.Tests.UnitTests.Dispatching
{
    public class MessageDispatcherTests
    {
        private record TestMessage(Guid Id);

        private class TestState : ISagaState
        {
            public bool Executed { get; set; }
            public Guid CorrelationId { get; set; } = default!;
        }

        private class TestSaga : ISagaHandler<TestMessage, TestState>
        {
            public Task HandleAsync(TestMessage message, TestState state, CancellationToken stoppingToken)
            {
                state.Executed = true;
                return Task.CompletedTask;
            }
        }

        private class TestConsumer : IMessageConsumer<TestMessage>
        {
            public static bool Called;

            public Task ConsumeAsync(TestMessage message, CancellationToken ct)
            {
                Called = true;
                return Task.CompletedTask;
            }
        }

        private class FailingConsumer : IMessageConsumer<TestMessage>
        {
            public Task ConsumeAsync(
                TestMessage message,
                CancellationToken ct)
            {
                throw new Exception("consumer failed");
            }
        }

        private class FailingRetryExecutor : IRetryExecutor
        {
            public Task ExecuteAsync(Func<Task> action, Func<Exception, int, Task> onFailure)
            {
                return onFailure(new Exception("fail"), 1);
            }
        }

        [Fact]
        public async Task Should_invoke_consumer()
        {
            // Arrange
            var services = new ServiceCollection();

            var registry = new MessageMetadataRegistry();
            registry.Register<TestMessage>("test", "group");

            var consumerRegistry = new ConsumerInvokerRegistry();
            consumerRegistry.Register<TestMessage>();

            services.AddSingleton(registry);
            services.AddSingleton(consumerRegistry);

            services.AddSingleton<IMessageTransport, InMemoryTransport>();

            services.AddScoped<TestConsumer>();
            services.AddScoped<IMessageConsumer<TestMessage>>(sp => sp.GetRequiredService<TestConsumer>());

            services.AddSingleton<IRetryExecutor, NoOpRetryExecutor>();

            var provider = services.BuildServiceProvider();

            var dispatcher = new MessageDispatcher(
                provider,
                registry,
                consumerRegistry,
                null);

            var metadata = registry.Get<TestMessage>();

            var envelope = new MessageEnvelope<TestMessage>
            {
                MessageId = Guid.NewGuid(),
                MessageType = metadata.Key,
                Payload = new TestMessage(Guid.NewGuid())
            };

            var json = JsonSerializer.Serialize(envelope);

            // Act
            await dispatcher.DispatchAsync(json, CancellationToken.None);

            // Assert
            TestConsumer.Called.Should().BeTrue();
        }

        [Fact]
        public async Task Should_throw_when_invalid_json()
        {
            var services = new ServiceCollection();

            services.AddSingleton(new MessageMetadataRegistry());
            services.AddSingleton(new ConsumerInvokerRegistry());
            services.AddSingleton<IMessageTransport, InMemoryTransport>();
            services.AddSingleton<IRetryExecutor, NoOpRetryExecutor>();

            var provider = services.BuildServiceProvider();

            var dispatcher = new MessageDispatcher(
                provider,
                provider.GetRequiredService<MessageMetadataRegistry>(),
                provider.GetRequiredService<ConsumerInvokerRegistry>(),
                null);

            var act = await Assert.ThrowsAnyAsync<Exception>(() => dispatcher.DispatchAsync("invalid-json", CancellationToken.None));
            act.Should().NotBeNull();
            act.Message.Should().NotBeNullOrEmpty();
        }


        [Fact]
        public async Task Should_not_invoke_when_no_consumer_registered()
        {
            var services = new ServiceCollection();

            var registry = new MessageMetadataRegistry();

            registry.Register<TestMessage>("test", "group");

            var consumerRegistry = new ConsumerInvokerRegistry();

            services.AddSingleton(registry);

            services.AddSingleton(consumerRegistry);

            services.AddSingleton<IMessageTransport, InMemoryTransport>();

            services.AddSingleton<IRetryExecutor, NoOpRetryExecutor>();

            var provider = services.BuildServiceProvider();

            var dispatcher = new MessageDispatcher(provider, registry, consumerRegistry, null);

            var envelope = new MessageEnvelope<TestMessage>
            {
                MessageId = Guid.NewGuid(),
                MessageType = registry.Get<TestMessage>().Key,
                Payload = new TestMessage(Guid.NewGuid())
            };

            var json = JsonSerializer.Serialize(envelope);

            await dispatcher.DispatchAsync(json, CancellationToken.None);

            consumerRegistry.HasHandler(typeof(TestMessage)).Should().BeFalse();
        }


        [Fact]
        public async Task Should_send_to_dlq_when_handler_fails()
        {
            var services = new ServiceCollection();

            var registry = new MessageMetadataRegistry();
            registry.Register<TestMessage>("test", "group");
            registry.Register<DeadLetterMessage>("dlq", "group");

            var consumerRegistry = new ConsumerInvokerRegistry();
            consumerRegistry.Register<TestMessage>();

            var transport = new Mock<IMessageTransport>();

            services.AddSingleton(registry);
            services.AddSingleton(consumerRegistry);
            services.AddSingleton(transport.Object);

            services.AddScoped<IMessageConsumer<TestMessage>, TestConsumer>();

            services.AddSingleton<IRetryExecutor, FailingRetryExecutor>();

            var provider = services.BuildServiceProvider();

            var dispatcher = new MessageDispatcher(provider, registry, consumerRegistry, null);

            var envelope = new MessageEnvelope<TestMessage>
            {
                MessageId = Guid.NewGuid(),
                MessageType = registry.Get<TestMessage>().Key,
                Payload = new TestMessage(Guid.NewGuid())
            };

            var json = JsonSerializer.Serialize(envelope);

            await dispatcher.DispatchAsync(json, CancellationToken.None);

            transport.Verify(x => x.SendAsync(
                It.IsAny<string>(),
                It.IsAny<string>(),
                It.IsAny<CancellationToken>()),
                Times.Once);
        }

        [Fact]
        public async Task Should_deserialize_payload_from_json_element()
        {
            var services = new ServiceCollection();

            var registry = new MessageMetadataRegistry();
            registry.Register<TestMessage>("test", "group");

            var consumerRegistry = new ConsumerInvokerRegistry();
            consumerRegistry.Register<TestMessage>();

            services.AddSingleton(registry);
            services.AddSingleton(consumerRegistry);
            services.AddSingleton<IMessageTransport, InMemoryTransport>();
            services.AddSingleton<IRetryExecutor, NoOpRetryExecutor>();

            services.AddScoped<IMessageConsumer<TestMessage>, TestConsumer>();

            var provider = services.BuildServiceProvider();

            var dispatcher = new MessageDispatcher(provider, registry, consumerRegistry, null);

            var envelope = new MessageEnvelope<TestMessage>
            {
                MessageId = Guid.Empty,
                MessageType = registry.Get<TestMessage>().Key,
                OccurredOn = DateTime.UtcNow,
                Payload = null!
            };

            var json = JsonSerializer.Serialize(envelope);

            await dispatcher.DispatchAsync(json, CancellationToken.None);

            TestConsumer.Called.Should().BeTrue();
        }

        [Fact]
        public async Task Should_throw_when_message_type_not_registered()
        {
            var services = new ServiceCollection();

            var registry = new MessageMetadataRegistry();

            var consumerRegistry = new ConsumerInvokerRegistry();

            services.AddSingleton(registry);

            services.AddSingleton(consumerRegistry);

            services.AddSingleton<IMessageTransport, InMemoryTransport>();

            services.AddSingleton<IRetryExecutor, NoOpRetryExecutor>();

            var provider = services.BuildServiceProvider();

            var dispatcher = new MessageDispatcher(provider, registry, consumerRegistry, null);

            var envelope = new MessageEnvelope<TestMessage>
            {
                MessageId = Guid.NewGuid(),
                MessageType = "not-registered",
                Payload = new TestMessage(Guid.NewGuid())
            };

            var json = JsonSerializer.Serialize(envelope);

            var act = async () => await dispatcher.DispatchAsync(json, CancellationToken.None);

            await act.Should().ThrowAsync<KeyNotFoundException>();
        }

        [Fact]
        public async Task Should_dispatch_saga_when_registered()
        {
            var services = new ServiceCollection();

            var registry = new MessageMetadataRegistry();

            registry.Register<TestMessage>("test", "group");

            registry.Register<DeadLetterMessage>("dlq", "group");

            var consumerRegistry = new ConsumerInvokerRegistry();

            var sagaRegistry = new SagaHandlerRegistry();

            sagaRegistry.Register<TestMessage, TestState>();

            services.AddSingleton(registry);

            services.AddSingleton(consumerRegistry);

            services.AddSingleton(sagaRegistry);

            services.AddSingleton<IMessageTransport, InMemoryTransport>();

            services.AddSingleton<IRetryExecutor, NoOpRetryExecutor>();

            services.AddSingleton<IIdempotencyStore, InMemoryIdempotencyStore>();

            services.AddSingleton<ISagaRepository<TestState>, InMemorySagaRepository<TestState>>();

            services.AddScoped<ISagaHandler<TestMessage, TestState>, TestSaga>();

            services.AddScoped<SagaDispatcher>();

            var provider = services.BuildServiceProvider();

            var dispatcher = new MessageDispatcher(provider, registry, consumerRegistry, sagaRegistry);

            var correlationId = Guid.NewGuid();

            var envelope = new MessageEnvelope<TestMessage>
            {
                MessageId = Guid.NewGuid(),
                MessageType = registry.Get<TestMessage>().Key,
                Payload = new TestMessage(correlationId)
            };

            var json = JsonSerializer.Serialize(envelope);

            await dispatcher.DispatchAsync(json, CancellationToken.None);

            var repository = provider.GetRequiredService<ISagaRepository<TestState>>();

            var state = await repository.GetAsync(correlationId, CancellationToken.None);

            state.Should().NotBeNull();
        }

        [Fact]
        public async Task Should_throw_when_deadletter_transport_fails()
        {
            var services = new ServiceCollection();

            var registry = new MessageMetadataRegistry();

            registry.Register<TestMessage>("test", "group");

            registry.Register<DeadLetterMessage>("dlq", "group");

            var consumerRegistry = new ConsumerInvokerRegistry();

            consumerRegistry.Register<TestMessage>();

            services.AddSingleton(registry);

            services.AddSingleton(consumerRegistry);

            services.AddScoped<IMessageConsumer<TestMessage>, FailingConsumer>();

            services.AddSingleton<IRetryExecutor>(
                new RetryExecutor(
                    new RetryPolicyOptions
                    {
                        MaxAttempts = 1,
                        Delays = []
                    }));

            var transport = new FakeTransport
            {
                Throw = true
            };

            services.AddSingleton<IMessageTransport>(transport);

            var provider = services.BuildServiceProvider();

            var dispatcher = new MessageDispatcher(provider, registry, consumerRegistry, null);

            var envelope = new MessageEnvelope<TestMessage>
            {
                MessageId = Guid.NewGuid(),
                MessageType = registry.Get<TestMessage>().Key,
                Payload = new TestMessage(Guid.NewGuid())
            };

            var json = JsonSerializer.Serialize(envelope);

            var act = async () => await dispatcher.DispatchAsync(json, CancellationToken.None);

            await act.Should().ThrowAsync<Exception>();
        }
    }
}
