using Confluent.Kafka;
using FerrumMalleator.Nuntium.Abstractions.Publishers;
using FerrumMalleator.Nuntium.Builders;
using FerrumMalleator.Nuntium.Sample.BasicFlow;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using OpenTelemetry.Metrics;
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
                options.BootstrapServers = "localhost:9092";

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

            bus.AddConsumer<PedidoConsumer, PedidoCriado>()
               .WithTopic<PedidoCriado>("pedido-criado", "basic-flow");
        });

        services.AddHostedService<SamplePublisherWorker>();
    });    

    var host = builder.Build();

    await host.RunAsync();
}
catch (Exception ex)
{
    Log.Fatal(ex, "BasicFlow foi encerrado inesperadamente.");
}
finally
{
    Log.CloseAndFlush();
}