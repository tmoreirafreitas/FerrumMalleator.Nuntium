using FerrumMalleator.Nuntium.Abstractions;
using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;

namespace FerrumMalleator.Nuntium.Tests.UnitTests.Saga;

public class CorrelationTests
{
    public record PedidoCriado(Guid MeuId);

    public class PedidoCorrelation : ISagaCorrelation<PedidoCriado>
    {
        public Guid GetCorrelationId(PedidoCriado message) => message.MeuId;
    }

    [Fact]
    public void Should_use_custom_correlation_resolver()
    {
        var services = new ServiceCollection();
        services.AddSingleton<ISagaCorrelation<PedidoCriado>, PedidoCorrelation>();

        var provider = services.BuildServiceProvider();

        var message = new PedidoCriado(Guid.NewGuid());

        var result = ResolveCorrelationId(message, provider);

        result.Should().Be(message.MeuId);
    }

    public record MessageWithCorrelation(Guid CorrelationId);

    [Fact]
    public void Should_use_correlation_id_property()
    {
        var provider = new ServiceCollection().BuildServiceProvider();

        var message = new MessageWithCorrelation(Guid.NewGuid());

        var result = ResolveCorrelationId(message, provider);

        result.Should().Be(message.CorrelationId);
    }

    public record PedidoCriado2(Guid PedidoId);

    [Fact]
    public void Should_use_property_that_ends_with_id()
    {
        var provider = new ServiceCollection().BuildServiceProvider();

        var message = new PedidoCriado2(Guid.NewGuid());

        var result = ResolveCorrelationId(message, provider);

        result.Should().Be(message.PedidoId);
    }

    public record GenericMessage(Guid Id);

    [Fact]
    public void Should_use_id_as_fallback()
    {
        var provider = new ServiceCollection().BuildServiceProvider();

        var message = new GenericMessage(Guid.NewGuid());

        var result = ResolveCorrelationId(message, provider);

        result.Should().Be(message.Id);
    }

    public record InvalidMessage(string Name);

    [Fact]
    public void Should_throw_when_no_correlation_found()
    {
        var provider = new ServiceCollection().BuildServiceProvider();

        var message = new InvalidMessage("teste");

        var act = () => ResolveCorrelationId(message, provider);

        act.Should().Throw<InvalidOperationException>();
    }

    private static object ResolveCorrelationId(object message, IServiceProvider provider)
    {
        var messageType = message.GetType();

        var customResolverType = typeof(ISagaCorrelation<>).MakeGenericType(messageType);

        var customResolver = provider.GetService(customResolverType);

        if (customResolver != null)
        {
            var method = customResolverType.GetMethod("GetCorrelationId");
            return method!.Invoke(customResolver, [message])!;
        }

        var prop = messageType.GetProperty("CorrelationId");

        if (prop != null)
            return prop.GetValue(message)!;

        var idProp = messageType
            .GetProperties()
            .FirstOrDefault(p => p.Name.EndsWith("Id", StringComparison.OrdinalIgnoreCase));

        if (idProp != null)
            return idProp.GetValue(message)!;

        var fallback = messageType.GetProperty("Id");

        if (fallback != null)
            return fallback.GetValue(message)!;

        throw new InvalidOperationException();
    }
}