using Confluent.Kafka;
using FerrumMalleator.Nuntium.Builders;
using FerrumMalleator.Nuntium.Persistence.EntityFramework.Builders;
using FerrumMalleator.Nuntium.Sample.OutboxFlow.Consumers;
using FerrumMalleator.Nuntium.Sample.OutboxFlow.Messages;
using FerrumMalleator.Nuntium.Sample.OutboxFlow.Persistence;
using FerrumMalleator.Nuntium.Sample.OutboxFlow.Workers;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using OpenTelemetry.Metrics;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;
using Serilog;

var builder = Host.CreateDefaultBuilder(args);

try
{
    builder.UseSerilog((context, logger) => logger.ReadFrom.Configuration(context.Configuration));

    builder.ConfigureServices((context, services) =>
    {
        services
            .AddOpenTelemetry()
            .ConfigureResource(resource =>
            {
                resource.AddService("Nuntium.Sample.OutboxFlow");
            })
            .WithTracing(tracing =>
            {
                tracing
                    .AddSource("FerrumMalleator.Nuntium")
                    .AddConsoleExporter();
            })
            .WithMetrics(metrics =>
            {
                metrics
                    .AddMeter("FerrumMalleator.Nuntium")
                    .AddConsoleExporter();
            });

        services.AddNuntium(bus =>
        {
            bus.UseKafka(options =>
            {
                options.BootstrapServers = "localhost:29092";

                options.ConfigureProducer(cp =>
                {
                    cp.Acks = Acks.All;
                    cp.LingerMs = 5;
                    cp.MessageTimeoutMs = 5000;
                });

                options.ConfigureConsumer(cc =>
                {
                    cc.FetchMinBytes = 1;
                    cc.AutoOffsetReset = AutoOffsetReset.Earliest;
                    cc.EnableAutoCommit = true;
                });
            });

            bus.UseOutbox();

            bus.AddConsumer<PedidoConsumer, PedidoCriado>();
            
            bus.WithTopic<PedidoCriado>("pedido-criado", "outbox-flow");

            bus.UseEfCorePersistence<OutboxFlowDbContext>(options =>
            {
                options.UseInMemoryDatabase("nuntium-outbox");
            });
        });

        services.AddHostedService<SamplePublisherWorker>();
    });

    var host = builder.Build();

    await host.RunAsync();
}
catch (Exception ex)
{
    Log.Fatal(ex, "OutboxFlow foi encerrado inesperadamente.");
}
finally
{
    Log.CloseAndFlush();
}