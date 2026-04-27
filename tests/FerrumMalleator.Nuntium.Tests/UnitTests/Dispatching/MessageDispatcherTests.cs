using FerrumMalleator.Nuntium.Abstractions;
using FerrumMalleator.Nuntium.Abstractions.Consumers;
using FerrumMalleator.Nuntium.Abstractions.Publishers;
using FerrumMalleator.Nuntium.Builders;
using FerrumMalleator.Nuntium.Dispatching;
using FerrumMalleator.Nuntium.Messaging.Envelopes;
using FerrumMalleator.Nuntium.Retry;
using FerrumMalleator.Nuntium.Transport.InMemory;
using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using System.Text.Json;

namespace FerrumMalleator.Nuntium.Tests.UnitTests.Dispatching
{
    public class MessageDispatcherTests
    {
        private record TestMessage(Guid Id);

        private class TestConsumer : IMessageConsumer<TestMessage>
        {
            public static bool Called;

            public Task ConsumeAsync(TestMessage message, CancellationToken ct)
            {
                Called = true;
                return Task.CompletedTask;
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
    }
}
