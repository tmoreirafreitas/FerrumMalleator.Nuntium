using FerrumMalleator.Nuntium.Abstractions.Consumers;
using FerrumMalleator.Nuntium.Builders;
using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Xunit.Sdk;

namespace FerrumMalleator.Nuntium.Tests.UnitTests.Consumers
{
    public class MessageConsumerTests
    {
        private class TestConsumer : IMessageConsumer<TestMessage>
        {
            public bool Called { get; private set; }

            public Task ConsumeAsync(TestMessage message, CancellationToken ct)
            {
                Called = true;
                return Task.CompletedTask;
            }
        }

        [Fact]
        public void Should_register_consumer()
        {
            var builder = new NuntiumBusBuilder(new ServiceCollection());

            builder.AddConsumer<TestConsumer, TestMessage>();

            builder.Consumers.Should().NotBeEmpty();
        }
    }
}
