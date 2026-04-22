using FerrumMalleator.Nuntium.Abstractions;
using FerrumMalleator.Nuntium.Abstractions.Persistence;
using FerrumMalleator.Nuntium.Abstractions.Publishers;
using FerrumMalleator.Nuntium.Configuration;
using FerrumMalleator.Nuntium.Dispatching;
using FerrumMalleator.Nuntium.Outbox;
using FerrumMalleator.Nuntium.Persistence.InMemory;
using FerrumMalleator.Nuntium.Resolution;
using FerrumMalleator.Nuntium.Sagas.Handlers;
using FerrumMalleator.Nuntium.Transport.Kafka;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using System;
using System.Linq;
using System.Reflection;

namespace FerrumMalleator.Nuntium.Builders
{
    public static class NuntiumMessagingExtensions
    {
        public static IServiceCollection AddNuntium(this IServiceCollection services, Action<NuntiumBusBuilder> configure)
        {
            var builder = new NuntiumBusBuilder(services);

            configure(builder);

            return Build(services, builder);
        }

        public static NuntiumBusBuilder UseInMemory(this NuntiumBusBuilder builder)
        {
            builder.MessagingOptions.PersistenceMode = PersistenceMode.InMemory;

            builder.Services.AddSingleton(typeof(ISagaRepository<>), typeof(InMemorySagaRepository<>));
            builder.Services.AddSingleton<IOutboxStore, InMemoryOutboxStore>();
            builder.Services.AddSingleton<IIdempotencyStore, InMemoryIdempotencyStore>();

            return builder;
        }

        public static NuntiumBusBuilder AddSaga(this NuntiumBusBuilder builder)
        {
            builder.MessagingOptions.EnableSaga = true;

            builder.Services.AddSingleton<SagaHandlerRegistry>();
            builder.Services.AddScoped<SagaDispatcher>();

            builder.Services.AddSagaHandlers();

            return builder;
        }

        public static NuntiumBusBuilder UseOutbox(this NuntiumBusBuilder builder)
        {
            builder.MessagingOptions.EnableOutbox = true;

            builder.Services.AddScoped<IMessagePublisher, OutboxPublisher>();
            builder.Services.AddHostedService<OutboxProcessor>();

            return builder;
        }

        public static NuntiumBusBuilder UseKafka(this NuntiumBusBuilder builder, Action<KafkaOptions> configure)
        {
            configure(builder.KafkaOptions);

            builder.Services.Configure<KafkaOptions>(opt =>
            {
                opt.BootstrapServers = builder.KafkaOptions.BootstrapServers;
                opt.SecurityProtocol = builder.KafkaOptions.SecurityProtocol;
                opt.SaslUsername = builder.KafkaOptions.SaslUsername;
                opt.SaslPassword = builder.KafkaOptions.SaslPassword;
            });

            builder.Services.AddSingleton<IMessagePublisher, KafkaPublisher>();            

            return builder;
        }

        private static IServiceCollection Build(IServiceCollection services, NuntiumBusBuilder busBuilder)
        {
            services.AddSingleton(busBuilder.Registry);
            services.AddSingleton(busBuilder.RetryPolicy);
            services.AddSingleton(busBuilder.ConsumerInvokerRegistry);
            services.AddScoped<MessageDispatcher>();
            services.AddSingleton<ITopicResolver>(new TopicResolver(busBuilder.Registry));

            foreach (var (Implementation, Service) in busBuilder.Consumers)
            {
                services.AddScoped(Service, Implementation);
            }

            var grouped = busBuilder.Registry.GetAll().GroupBy(x => x.GroupId);
            foreach (var group in grouped)
            {
                services.AddHostedService(provider =>
                    new KafkaConsumerWorker(
                        provider,
                        provider.GetRequiredService<IOptions<KafkaOptions>>(),
                        group.Key,
                        group.Select(x => x.Topic)));
            }
            return services;
        }
    }
}
