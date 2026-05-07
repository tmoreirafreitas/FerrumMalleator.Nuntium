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
        public static bool Received { get; private set; }
        public static void Reset() { Received = false; }
        public Task ConsumeAsync(Pedido message, CancellationToken ct)
        {
            Received = true;
            return Task.CompletedTask;
        }
    }


    [Fact]
    public async Task Should_publish_and_consume_message()
    {
        PedidoConsumer.Reset();

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

        var publisher =
            host.Services.GetRequiredService<IMessagePublisher>();

        await publisher.PublishAsync(
            new Pedido(Guid.NewGuid()));

        var timeout = TimeSpan.FromSeconds(10);

        var start = DateTime.UtcNow;

        while (!PedidoConsumer.Received &&
               DateTime.UtcNow - start < timeout)
        {
            await Task.Delay(100);
        }

        PedidoConsumer.Received.Should().BeTrue();

        await host.StopAsync();
    }
}