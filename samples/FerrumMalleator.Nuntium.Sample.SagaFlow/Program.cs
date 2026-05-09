using Confluent.Kafka;
using FerrumMalleator.Nuntium.Builders;
using FerrumMalleator.Nuntium.Persistence.EntityFramework.Builders;
using FerrumMalleator.Nuntium.Sample.SagaFlow.Consumers;
using FerrumMalleator.Nuntium.Sample.SagaFlow.Messages;
using FerrumMalleator.Nuntium.Sample.SagaFlow.Persistence;
using FerrumMalleator.Nuntium.Sample.SagaFlow.Workers;
using Microsoft.EntityFrameworkCore;
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
            })
            .UseRetry(cf =>
            {
                cf.MaxAttempts = 4;
                cf.Delays = [TimeSpan.FromSeconds(5), TimeSpan.FromSeconds(10), TimeSpan.FromSeconds(20), TimeSpan.FromSeconds(40)];
            });

            bus.AddSaga()
                .WithTopic<PedidoCriado>("pedido-criado", "saga-flow")
                .WithTopic<AprovarPagamento>("aprovar-pagamento", "saga-flow")
                .WithTopic<PagamentoAprovado>("pagamento-aprovado", "saga-flow")
                .WithTopic<SepararEstoque>("separar-estoque", "saga-flow")
                .WithTopic<EstoqueFinalizado>("estoque-finalizado", "saga-flow");

            bus.AddConsumer<PedidoConsumer, AprovarPagamento>()
               .AddConsumer<PedidoConsumer, SepararEstoque>();

            bus.UseEfCorePersistence<SagaFlowDbContext>(opt =>
            {
                opt.UseInMemoryDatabase("SagaFlowDb");
            });
        });

        services.AddHostedService<SamplePublisherWorker>();
    });

    var host = builder.Build();

    await host.RunAsync();
}
catch (Exception ex)
{
    Log.Fatal(ex, "SagaFlow foi encerrado inesperadamente.");
}
finally
{
    Log.CloseAndFlush();
}