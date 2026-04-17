using FerrumMalleator.Nuntium.Abstractions.Consumers;
using FerrumMalleator.Nuntium.Abstractions.Publishers;
using FerrumMalleator.Nuntium.Builders;
using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace FerrumMalleator.Nuntium.Tests.IntegrationTests;

public class FlowTests
{
    public record Pedido(Guid PedidoId);

    public class PedidoConsumer : IMessageConsumer<Pedido>
    {
        public static bool Received;

        public Task ConsumeAsync(Pedido message, CancellationToken ct)
        {
            Received = true;
            return Task.CompletedTask;
        }
    }


    [Fact]
    public async Task Should_publish_and_consume_message()
    {
        PedidoConsumer.Received = false;

        var host = Host.CreateDefaultBuilder()
            .ConfigureServices(services =>
            {
                services.AddNuntium(bus =>
                {
                    bus.UseInMemory()
                       .UseOutbox()
                       .AddConsumer<PedidoConsumer, Pedido>()
                       .WithTopic<Pedido>("pedido", "group");
                });
            })
            .Build();

        await host.StartAsync();

        var publisher = host.Services.GetRequiredService<IMessagePublisher>();

        await publisher.PublishAsync(new Pedido(Guid.NewGuid()));

        await Task.Delay(1500);

        PedidoConsumer.Received.Should().BeTrue();

        await host.StopAsync();
    }
}