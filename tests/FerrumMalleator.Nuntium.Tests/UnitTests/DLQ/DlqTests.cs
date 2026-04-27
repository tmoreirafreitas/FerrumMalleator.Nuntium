using FerrumMalleator.Nuntium.Abstractions;
using FerrumMalleator.Nuntium.Abstractions.Consumers;
using FerrumMalleator.Nuntium.Abstractions.Publishers;
using FerrumMalleator.Nuntium.Builders;
using FerrumMalleator.Nuntium.Configuration;
using FerrumMalleator.Nuntium.Dispatching;
using FerrumMalleator.Nuntium.Messaging.Envelopes;
using FerrumMalleator.Nuntium.Messaging.Models;
using FerrumMalleator.Nuntium.Retry;
using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using System.Text.Json;

namespace FerrumMalleator.Nuntium.Tests.UnitTests.DLQ
{
    public class DlqTests
    {
        internal record TestMessage(Guid Id);
        internal class FailingConsumer : IMessageConsumer<TestMessage>
        {
            public Task ConsumeAsync(TestMessage message, CancellationToken ct) => throw new Exception("fail");
        }

        internal class FakeTransport : IMessageTransport
        {
            public static bool Sent;

            public Task SendAsync(string topic, string message, CancellationToken ct)
            {
                Sent = true;
                return Task.CompletedTask;
            }
        }

        [Fact]
        public async Task Should_send_to_dlq_on_failure()
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

            services.AddSingleton<IRetryExecutor>(new RetryExecutor(new RetryPolicyOptions
            {
                MaxAttempts = 1,
                Delays = []
            }));

            services.AddSingleton<IMessageTransport, FakeTransport>();

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

            await Assert.ThrowsAsync<Exception>(async () => await dispatcher.DispatchAsync(json, CancellationToken.None));

            FakeTransport.Sent.Should().BeTrue();
        }
    }
}
