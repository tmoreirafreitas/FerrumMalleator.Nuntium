using FerrumMalleator.Nuntium.Abstractions.Consumers;
using FerrumMalleator.Nuntium.Dispatching;
using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;

namespace FerrumMalleator.Nuntium.Tests.UnitTests.Dispatching;

public class ConsumerInvokerRegistryTests
{
    private record TestMessage;

    private class TestConsumer : IMessageConsumer<TestMessage>
    {
        public bool Called { get; private set; }

        public Task ConsumeAsync(TestMessage message, CancellationToken ct)
        {
            Called = true;
            return Task.CompletedTask;
        }
    }

    private class FailingConsumer : IMessageConsumer<TestMessage>
    {
        public Task ConsumeAsync(TestMessage message, CancellationToken ct)
        {
            throw new InvalidOperationException("consumer failed");
        }
    }

    [Fact]
    public async Task Should_invoke_registered_consumer()
    {
        var services = new ServiceCollection();
        services.AddScoped<TestConsumer>();

        services.AddScoped<IMessageConsumer<TestMessage>>(sp => sp.GetRequiredService<TestConsumer>());

        var provider = services.BuildServiceProvider();

        var registry = new ConsumerInvokerRegistry();
        registry.Register<TestMessage>();

        using var scope = provider.CreateScope();

        var consumer = scope.ServiceProvider.GetRequiredService<TestConsumer>();

        await registry.Invoke(typeof(TestMessage), scope.ServiceProvider, new TestMessage(), CancellationToken.None);

        consumer.Called.Should().BeTrue();
    }

    [Fact]
    public async Task Should_throw_when_consumer_fails()
    {
        var services = new ServiceCollection();

        services.AddScoped<IMessageConsumer<TestMessage>, FailingConsumer>();

        var provider = services.BuildServiceProvider();

        var registry = new ConsumerInvokerRegistry();

        registry.Register<TestMessage>();

        using var scope = provider.CreateScope();

        var act = async () => await registry.Invoke(typeof(TestMessage), scope.ServiceProvider, new TestMessage(), CancellationToken.None);

        await act.Should().ThrowAsync<InvalidOperationException>().WithMessage("consumer failed");
    }
}