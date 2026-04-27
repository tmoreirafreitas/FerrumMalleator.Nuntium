using FerrumMalleator.Nuntium.Abstractions;
using FerrumMalleator.Nuntium.Abstractions.Consumers;
using FerrumMalleator.Nuntium.Abstractions.Persistence;
using FerrumMalleator.Nuntium.Abstractions.Publishers;
using FerrumMalleator.Nuntium.Builders;
using FerrumMalleator.Nuntium.Outbox;
using FerrumMalleator.Nuntium.Persistence.EntityFramework.Builders;
using FerrumMalleator.Nuntium.Persistence.EntityFramework.Context;
using FerrumMalleator.Nuntium.Sagas.Handlers;
using FluentAssertions;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace FerrumMalleator.Nuntium.Tests.IntegrationTests
{
    public class FullEfIntegrationTests
    {
        private record Pedido(Guid PedidoId);
        private record PedidoCriado(Guid PedidoId);
        private record PagamentoAprovado(Guid PedidoId);

        private class PedidoState : ISagaState
        {
            public bool Criado { get; set; }
            public bool Pago { get; set; }
            public Guid CorrelationId { get; set; }
        }

        private class TestDbContext(DbContextOptions options) : NuntiumDbContext(options)
        {
            public DbSet<PedidoState> PedidoSagas { get; set; }
            protected override void OnConfigureNuntium(ModelBuilder modelBuilder)
            {
                modelBuilder.Entity<PedidoState>(e =>
                {
                    e.HasKey(x => x.CorrelationId);
                    e.Property(x => x.CorrelationId).ValueGeneratedNever();
                    e.Property(x => x.Criado).IsRequired();
                    e.Property(x => x.Pago).IsRequired();
                });
            }
        }

        private class PedidoConsumer : IMessageConsumer<PedidoCriado>
        {
            public static bool Called;

            public Task ConsumeAsync(PedidoCriado message, CancellationToken ct)
            {
                Called = true;
                return Task.CompletedTask;
            }
        }

        private class PedidoSaga : ISagaHandler<PedidoCriado, PedidoState>, ISagaHandler<PagamentoAprovado, PedidoState>
        {
            public Task HandleAsync(PedidoCriado message, PedidoState state, CancellationToken ct)
            {
                state.Criado = true;
                return Task.CompletedTask;
            }

            public Task HandleAsync(PagamentoAprovado message, PedidoState state, CancellationToken ct)
            {
                state.Pago = true;
                return Task.CompletedTask;
            }
        }

        [Fact]
        public async Task Should_process_message_and_persist_saga_state()
        {
            PedidoConsumer.Called = false;

            var host = Host.CreateDefaultBuilder()
            .ConfigureServices(services =>
            {
                services.AddNuntium(bus =>
                {
                    bus.UseRetry(cf =>
                    {
                        cf.MaxAttempts = 1;
                        cf.Delays = [];
                    })
                    .UseOutbox()
                    .AddSaga()
                    .AddConsumer<PedidoConsumer, PedidoCriado>()
                    .WithTopic<PedidoCriado>("pedido-criado", "group")
                    .WithTopic<PagamentoAprovado>("pagamento-aprovado", "group")
                    .UseInMemoryTransport()
                    .UseEfCorePersistence<TestDbContext>(opt =>
                    {
                        opt.UseInMemoryDatabase("TestNuntiumDb");
                    });
                });
            })
            .Build();

            await host.StartAsync();

            var publisher = host.Services.GetRequiredService<IMessagePublisher>();

            var pedidoId = Guid.NewGuid();

            // Act
            await publisher.PublishAsync(new PedidoCriado(pedidoId));

            // força processamento do outbox
            var processor = host.Services.GetServices<IHostedService>()
                .OfType<OutboxProcessor>()
                .First();

            await processor.StartAsync(CancellationToken.None);

            await Task.Delay(1000);

            // Assert consumer
            PedidoConsumer.Called.Should().BeTrue();

            // Assert saga
            var repo = host.Services.GetRequiredService<ISagaRepository<PedidoState>>();

            var state = await repo.GetAsync(pedidoId, CancellationToken.None);

            state.Should().NotBeNull();
            state!.Criado.Should().BeTrue();
        }
    }
}
