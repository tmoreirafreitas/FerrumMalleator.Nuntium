using FerrumMalleator.Nuntium.Abstractions;
using FerrumMalleator.Nuntium.Abstractions.Persistence;
using FerrumMalleator.Nuntium.Abstractions.Publishers;
using FerrumMalleator.Nuntium.Builders;
using FerrumMalleator.Nuntium.Dispatching;
using FerrumMalleator.Nuntium.Messaging.Envelopes;
using FerrumMalleator.Nuntium.Messaging.Models;
using FerrumMalleator.Nuntium.Persistence.InMemory;
using FerrumMalleator.Nuntium.Retry;
using FerrumMalleator.Nuntium.Sagas.Handlers;
using FerrumMalleator.Nuntium.Transport.InMemory;
using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Moq;
using System.Text.Json;

namespace FerrumMalleator.Nuntium.Tests.UnitTests.Sagas
{
    public class SagaDispatcherTests
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

        private class FailingSaga : ISagaHandler<TestMessage, TestState>
        {
            public Task HandleAsync(TestMessage message, TestState state, CancellationToken ct)
                => throw new Exception("fail");
        }

        [Fact]
        public async Task Should_execute_saga_when_registered()
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

            services.AddScoped<MessageDispatcher>();
            services.AddScoped<SagaDispatcher>();
            services.AddScoped<ISagaHandler<TestMessage, TestState>, TestSaga>();

            services.AddSingleton<IRetryExecutor, NoOpRetryExecutor>();
            services.AddSingleton<IIdempotencyStore, InMemoryIdempotencyStore>();
            services.AddSingleton<ISagaRepository<TestState>, InMemorySagaRepository<TestState>>();
            services.AddSingleton<IMessageTransport, InMemoryTransport>();

            var provider = services.BuildServiceProvider();

            var dispatcher = new MessageDispatcher(
                provider,
                registry,
                consumerRegistry,
                sagaRegistry);

            var metadata = registry.Get<TestMessage>();

            var envelope = new MessageEnvelope<TestMessage>
            {
                MessageId = Guid.NewGuid(),
                MessageType = metadata.Key,
                Payload = new TestMessage(Guid.NewGuid())
            };

            var json = JsonSerializer.Serialize(envelope);

            await dispatcher.DispatchAsync(json, CancellationToken.None);

            var repo = provider.GetRequiredService<ISagaRepository<TestState>>();
            var state = await repo.GetAsync(envelope.Payload.Id, CancellationToken.None);

            state.Should().NotBeNull();
            state!.Executed.Should().BeTrue();
        }


        [Fact]
        public async Task Should_ignore_when_no_saga_registered()
        {
            var services = new ServiceCollection();

            var registry = new MessageMetadataRegistry();
            registry.Register<TestMessage>("test", "group");
            registry.Register<DeadLetterMessage>("dlq", "group");

            var consumerRegistry = new ConsumerInvokerRegistry();

            services.AddSingleton(registry);
            services.AddSingleton(consumerRegistry);

            services.AddScoped<MessageDispatcher>();
            services.AddScoped<SagaDispatcher>();
            services.AddScoped<ISagaHandler<TestMessage, TestState>, TestSaga>();

            services.AddSingleton<IRetryExecutor, NoOpRetryExecutor>();
            services.AddSingleton<IIdempotencyStore, InMemoryIdempotencyStore>();

            services.AddSingleton<ISagaRepository<TestState>, InMemorySagaRepository<TestState>>();

            services.AddSingleton<IMessageTransport, InMemoryTransport>();

            var provider = services.BuildServiceProvider();

            var dispatcher = new MessageDispatcher(provider, registry, consumerRegistry, null);

            var correlationId = Guid.NewGuid();

            var metadata = registry.Get<TestMessage>();

            var envelope = new MessageEnvelope<TestMessage>
            {
                MessageId = Guid.NewGuid(),
                MessageType = metadata.Key,
                Payload = new TestMessage(correlationId)
            };

            var json = JsonSerializer.Serialize(envelope);

            await dispatcher.DispatchAsync(json, CancellationToken.None);

            var repository = provider.GetRequiredService<ISagaRepository<TestState>>();

            var state = await repository.GetAsync(correlationId, CancellationToken.None);

            state.Should().BeNull();
        }

        [Fact]
        public async Task Should_not_execute_when_already_processed()
        {
            var idempotency = new Mock<IIdempotencyStore>();

            idempotency.Setup(x => x.HasProcessedAsync(It.IsAny<Guid>(), It.IsAny<CancellationToken>()))
                       .ReturnsAsync(true);

            var registry = new MessageMetadataRegistry();
            registry.Register<TestMessage>("test", "group");
            registry.Register<DeadLetterMessage>("dlq", "group");

            var (dispatcher, metadata) = BuildDispatcher(idempotency: idempotency.Object);

            var envelope = new MessageEnvelope<TestMessage>
            {
                MessageId = Guid.NewGuid(),
                MessageType = metadata.Key,
                Payload = new TestMessage(Guid.NewGuid())
            };

            await dispatcher.DispatchAsync(envelope, typeof(TestMessage), envelope.Payload!, CancellationToken.None);

            idempotency.Verify(x => x.MarkProcessedAsync(It.IsAny<Guid>(), It.IsAny<CancellationToken>()), Times.Never);
        }

        private static (SagaDispatcher, MessageMetadata) BuildDispatcher(Type? saga = null, IMessageTransport? transport = null, IIdempotencyStore? idempotency = null)
        {
            var services = new ServiceCollection();

            var registry = new MessageMetadataRegistry();
            registry.Register<TestMessage>("test", "group");
            registry.Register<DeadLetterMessage>("dlq", "group");

            var sagaRegistry = new SagaHandlerRegistry();

            if (saga != null)
                sagaRegistry.Register<TestMessage, TestState>();

            services.AddSingleton(registry);
            services.AddSingleton(sagaRegistry);

            services.AddSingleton(idempotency ?? new InMemoryIdempotencyStore());
            services.AddSingleton<IRetryExecutor, NoOpRetryExecutor>();
            services.AddSingleton<ISagaRepository<TestState>, InMemorySagaRepository<TestState>>();
            services.AddSingleton<IMessageTransport, InMemoryTransport>();

            if (saga == typeof(FailingSaga))
                services.AddScoped<ISagaHandler<TestMessage, TestState>, FailingSaga>();
            else
                services.AddScoped<ISagaHandler<TestMessage, TestState>, TestSaga>();

            var provider = services.BuildServiceProvider();

            var metadata = registry.Get<TestMessage>();
            var dispatcher = new SagaDispatcher(
                provider,
                sagaRegistry,
                provider.GetRequiredService<IIdempotencyStore>(),
                provider.GetRequiredService<IRetryExecutor>(),
                provider.GetRequiredService<IMessageTransport>(),
                registry);

            return (dispatcher, metadata!);
        }
    }
}
