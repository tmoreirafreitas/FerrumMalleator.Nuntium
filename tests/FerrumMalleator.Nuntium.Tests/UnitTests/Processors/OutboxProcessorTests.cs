using FerrumMalleator.Nuntium.Abstractions;
using FerrumMalleator.Nuntium.Abstractions.Persistence;
using FerrumMalleator.Nuntium.Abstractions.Publishers;
using FerrumMalleator.Nuntium.Builders;
using FerrumMalleator.Nuntium.Configuration;
using FerrumMalleator.Nuntium.Dispatching;
using FerrumMalleator.Nuntium.Messaging.Models;
using FerrumMalleator.Nuntium.Outbox;
using FerrumMalleator.Nuntium.Persistence.InMemory;
using FerrumMalleator.Nuntium.Retry;
using FerrumMalleator.Nuntium.Tests.Fake;
using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using System.Text.Json;

namespace FerrumMalleator.Nuntium.Tests.UnitTests.Processors
{
    public class OutboxProcessorTests
    {
        private record TestMessage(Guid Id);

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

            var transporte = new FakeTransport();
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
            FakeTransport.Sent.Should().BeTrue();

            var pendings = await store.GetPendingAsync(1, CancellationToken.None);
            pendings.Should().BeEmpty();
        }
    }
}
