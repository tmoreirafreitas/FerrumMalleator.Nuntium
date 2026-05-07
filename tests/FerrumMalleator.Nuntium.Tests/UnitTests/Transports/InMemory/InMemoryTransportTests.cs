using FerrumMalleator.Nuntium.Abstractions;
using FerrumMalleator.Nuntium.Builders;
using FerrumMalleator.Nuntium.Dispatching;
using FerrumMalleator.Nuntium.Retry;
using FerrumMalleator.Nuntium.Transport.InMemory;
using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;

namespace FerrumMalleator.Nuntium.Tests.UnitTests.Transports.InMemory
{
    public class InMemoryTransportTests
    {
        [Fact]
        public async Task Should_throw_when_dispatch_fails()
        {
            var services = new ServiceCollection();

            var registry = new MessageMetadataRegistry();

            var consumerRegistry = new ConsumerInvokerRegistry();

            services.AddSingleton(registry);

            services.AddSingleton(consumerRegistry);

            services.AddSingleton<IRetryExecutor, NoOpRetryExecutor>();

            services.AddScoped<MessageDispatcher>();

            var provider = services.BuildServiceProvider();

            var transport = new InMemoryTransport(provider);

            var act = async () => await transport.SendAsync("invalid-message", "{}", CancellationToken.None);

            await act.Should().ThrowAsync<Exception>();
        }
    }
}
