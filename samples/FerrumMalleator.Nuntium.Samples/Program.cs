using Confluent.Kafka;
using FerrumMalleator.Nuntium.Builders;
using FerrumMalleator.Nuntium.Persistence.EntityFramework.Builders;
using FerrumMalleator.Nuntium.Persistence.EntityFramework.Context;
using FerrumMalleator.Nuntium.Samples;
using FerrumMalleator.Nuntium.Samples.Consumers;
using FerrumMalleator.Nuntium.Samples.Messages;
using FerrumMalleator.Nuntium.Samples.Persistence.Context;
using Microsoft.EntityFrameworkCore;

var builder = Host.CreateApplicationBuilder(args);

builder.Services.AddNuntium(bus =>
{
    bus.UseKafka(opt =>
    {
        opt.BootstrapServers = builder.Configuration["Kafka:BootstrapServers"]!;

        opt.ConfigureProducer(cp =>
        {
            cp.Acks = Acks.All;
            cp.LingerMs = 5;
            cp.MessageTimeoutMs = 5000;
        });

        opt.ConfigureConsumer(cc =>
        {
            cc.FetchMinBytes = 1;
            cc.AutoOffsetReset = AutoOffsetReset.Earliest;
            cc.EnableAutoCommit = true;
        });
    })
    .UseRetry(cf =>
    {
        cf.MaxAttempts = 4;
        cf.Delays = [
            TimeSpan.FromSeconds(5),
                TimeSpan.FromSeconds(10),
                TimeSpan.FromSeconds(20),
                TimeSpan.FromSeconds(40)
        ];
    })
    .UseOutbox()
    .AddSaga()
    .AddConsumer<PedidoConsumer, PedidoCriado>()
    .WithTopic<Pedido>("pedido", "pedido-group")
    .WithTopic<PedidoCriado>("pedido-criado", "pedido-group")
    .WithTopic<PagamentoAprovado>("pagamento-aprovado", "pedido-group")
    .WithTopic<PedidoFinalizado>("pedido-finalizado", "pedido-group")
    .UsePersistence<SamplesDbContext>(opt =>
    {
        opt.UseInMemoryDatabase("SampleNuntiumDb");
    });
});

builder.Services.AddHostedService<SamplePublisherWorker>();
//builder.Services.AddHostedService<OutBoxServiceWorker>();

var host = builder.Build();

await host.RunAsync();