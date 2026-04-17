using FerrumMalleator.Nuntium.Builders;
using FerrumMalleator.Nuntium.Samples;
using FerrumMalleator.Nuntium.Samples.Consumers;
using FerrumMalleator.Nuntium.Samples.Messages;


var builder = Host.CreateApplicationBuilder(args);

builder.Services.AddNuntium(bus =>
{
    bus.UseKafka(opt =>
    {
        opt.BootstrapServers = builder.Configuration["Kafka:BootstrapServers"];
    })
    .UseInMemory()
    .WithTopic<Pedido>("pedido.criado", "pedido-group")
    .AddConsumer<PedidoConsumer, Pedido>();
});

builder.Services.AddHostedService<SamplePublisherWorker>();

var host = builder.Build();
await host.RunAsync();