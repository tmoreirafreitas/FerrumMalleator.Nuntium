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
using System.Text.Json;

namespace FerrumMalleator.Nuntium.Tests.UnitTests.Saga
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
            public Task Handle(TestMessage message, TestState state, CancellationToken stoppingToken)
            {
                state.Executed = true;
                return Task.CompletedTask;
            }
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
    }
}
