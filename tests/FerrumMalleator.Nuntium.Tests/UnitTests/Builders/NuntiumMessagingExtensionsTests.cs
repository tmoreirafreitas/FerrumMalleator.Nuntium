using FerrumMalleator.Nuntium.Abstractions;
using FerrumMalleator.Nuntium.Abstractions.Consumers;
using FerrumMalleator.Nuntium.Abstractions.Publishers;
using FerrumMalleator.Nuntium.Builders;
using FerrumMalleator.Nuntium.DeadLetter;
using FerrumMalleator.Nuntium.Messaging.Models;
using FerrumMalleator.Nuntium.Outbox;
using FerrumMalleator.Nuntium.Retry;
using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;

namespace FerrumMalleator.Nuntium.Tests.UnitTests.Builders
{
    public class NuntiumMessagingExtensionsTests
    {
        private record TestMessage(Guid Id);

        private class TestConsumer : IMessageConsumer<TestMessage>
        {
            public Task ConsumeAsync(TestMessage message, CancellationToken ct)
                => Task.CompletedTask;
        }
        
        [Fact]
        public void Should_register_deadletter_when_not_present()
        {
            var services = new ServiceCollection();

            services.AddNuntium(bus =>
            {
                bus.UseInMemory();
            });

            var provider = services.BuildServiceProvider();

            var registry = provider.GetRequiredService<MessageMetadataRegistry>();

            registry.Contains<DeadLetterMessage>().Should().BeTrue();
        }
                
        [Fact]
        public void Should_register_noop_retry_when_not_configured()
        {
            var services = new ServiceCollection();

            services.AddNuntium(bus =>
            {
                bus.UseInMemory();
            });

            var provider = services.BuildServiceProvider();

            var retry = provider.GetRequiredService<IRetryExecutor>();

            retry.Should().BeOfType<NoOpRetryExecutor>();
        }

        [Fact]
        public void Should_resolve_publisher_when_no_outbox()
        {
            var services = new ServiceCollection();

            services.AddNuntium(bus =>
            {
                bus.UseInMemory();
            });

            var provider = services.BuildServiceProvider();

            var publisher = provider.GetRequiredService<IMessagePublisher>();

            publisher.Should().NotBeNull();
        }

        [Fact]
        public void Should_use_outbox_publisher_when_enabled()
        {
            var services = new ServiceCollection();

            services.AddNuntium(bus =>
            {
                bus.UseInMemory()
                   .UseOutbox();
            });

            var provider = services.BuildServiceProvider();

            var publisher = provider.GetRequiredService<IMessagePublisher>();

            publisher.Should().BeOfType<OutboxPublisher>();
        }
        
        [Fact]
        public void Should_register_deadletter_reprocessor_when_store_exists()
        {
            var services = new ServiceCollection();

            services.AddNuntium(bus =>
            {
                bus.UseInMemory();
            });

            services.Any(s => s.ImplementationType == typeof(DeadLetterReprocessor))
                .Should().BeTrue();
        }
        
        [Fact]
        public void Should_register_consumers()
        {
            var services = new ServiceCollection();

            services.AddNuntium(bus =>
            {
                bus.UseInMemory()
                   .AddConsumer<TestConsumer, TestMessage>()
                   .WithTopic<TestMessage>("test-topic", "group");
            });

            var provider = services.BuildServiceProvider();

            var consumer = provider.GetService<IMessageConsumer<TestMessage>>();

            consumer.Should().NotBeNull();
        }
    }
}