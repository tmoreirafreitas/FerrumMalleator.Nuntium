using FerrumMalleator.Nuntium.Abstractions;
using FerrumMalleator.Nuntium.Abstractions.Persistence;
using FerrumMalleator.Nuntium.Abstractions.Publishers;
using FerrumMalleator.Nuntium.Configuration;
using FerrumMalleator.Nuntium.DeadLetter;
using FerrumMalleator.Nuntium.Dispatching;
using FerrumMalleator.Nuntium.Messaging.Models;
using FerrumMalleator.Nuntium.Outbox;
using FerrumMalleator.Nuntium.Persistence.InMemory;
using FerrumMalleator.Nuntium.Resolution;
using FerrumMalleator.Nuntium.Retry;
using FerrumMalleator.Nuntium.Sagas.Handlers;
using FerrumMalleator.Nuntium.Transport.InMemory;
using FerrumMalleator.Nuntium.Transport.Kafka;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

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
            return builder
                .UseInMemoryTransport()
                .UseInMemoryPersistence();
        }

        public static NuntiumBusBuilder UseInMemoryPersistence(this NuntiumBusBuilder builder)
        {
            builder.MessagingOptions.PersistenceMode = PersistenceMode.InMemory;

            builder.Services.AddSingleton(typeof(ISagaRepository<>), typeof(InMemorySagaRepository<>));
            builder.Services.AddSingleton<IOutboxStore, InMemoryOutboxStore>();
            builder.Services.AddSingleton<IIdempotencyStore, InMemoryIdempotencyStore>();
            builder.Services.AddSingleton<IDeadLetterStore, InMemoryDeadLetterStore>();

            return builder;
        }

        public static NuntiumBusBuilder UseRetry(this NuntiumBusBuilder builder, Action<RetryPolicyOptions> configure)
        {
            configure(builder.RetryPolicy);
            builder.Services.AddSingleton(builder.RetryPolicy);
            builder.Services.AddSingleton<IRetryExecutor, RetryExecutor>();
            return builder;
        }

        public static NuntiumBusBuilder AddSaga(this NuntiumBusBuilder builder, Action<SagaOptions>? configure = null)
        {
            builder.MessagingOptions.EnableSaga = true;

            configure?.Invoke(builder.SagaOptions);

            builder.Services.AddSingleton<SagaHandlerRegistry>();
            builder.Services.AddScoped<SagaDispatcher>();

            return builder;
        }

        public static NuntiumBusBuilder UseOutbox(this NuntiumBusBuilder builder)
        {
            builder.MessagingOptions.EnableOutbox = true;

            builder.Services.AddHostedService<OutboxProcessor>();

            return builder;
        }

        public static NuntiumBusBuilder UseInMemoryTransport(this NuntiumBusBuilder builder)
        {
            builder.Services.AddSingleton<IMessageTransport, InMemoryTransport>();
            return builder;
        }

        public static NuntiumBusBuilder UseKafka(this NuntiumBusBuilder builder, Action<KafkaOptions> configure)
        {
            builder.Services.Configure(configure);

            builder.Services.AddSingleton<IKafkaProducerFactory, KafkaProducerFactory>();
            builder.Services.AddSingleton<KafkaPublisher>();
            builder.Services.AddSingleton<IMessageTransport>(sp => sp.GetRequiredService<KafkaPublisher>());
            builder.Services.AddSingleton<IKafkaAdminClient, KafkaAdminClient>();
            builder.Services.AddHostedService<KafkaTopicProvisioner>();

            return builder;
        }

        private static IServiceCollection Build(IServiceCollection services, NuntiumBusBuilder busBuilder)
        {
            if (!busBuilder.Registry.Contains<DeadLetterMessage>())
            {
                busBuilder.Registry.Register<DeadLetterMessage>(
                    DefaultTopics.DeadLetterTopic,
                    DefaultTopics.DeadLetterGroup);
            }

            services.AddScoped<MessageDispatcher>();
            services.AddSingleton(busBuilder.Registry);
            services.AddSingleton(busBuilder.ConsumerInvokerRegistry);
            services.AddSingleton<ITopicResolver>(new TopicResolver(busBuilder.Registry));

            services.AddScoped(sp =>
            {
                var options = busBuilder.MessagingOptions;

                if (options.EnableOutbox)
                {
                    return ActivatorUtilities.CreateInstance<OutboxPublisher>(sp);
                }

                return sp.GetRequiredService<IMessageTransport>() as IMessagePublisher
                       ?? throw new InvalidOperationException("Transport must implement IMessagePublisher");
            });

            if (!services.Any(s => s.ServiceType == typeof(IRetryExecutor)))
            {
                services.AddSingleton<IRetryExecutor, NoOpRetryExecutor>();
            }

            if (services.Any(s => s.ServiceType == typeof(IDeadLetterStore)))
            {
                services.AddHostedService<DeadLetterReprocessor>();
            }

            foreach (var (Implementation, Service) in busBuilder.Consumers)
            {
                services.AddScoped(Implementation);
                services.AddScoped(Service, sp => sp.GetRequiredService(Implementation));
            }

            var grouped = busBuilder.Registry.GetAll().GroupBy(x => x.GroupId);

            foreach (var group in grouped)
            {
                services.AddSingleton<IKafkaConsumer, KafkaConsumerAdapter>(sp =>
                {
                    var options = sp.GetRequiredService<IOptions<KafkaOptions>>();
                    return new KafkaConsumerAdapter(options, group.Key);
                });

                services.AddHostedService(provider =>
                    new KafkaConsumerWorker(
                        provider,
                        group.Key,
                        group.Select(x => x.Topic)));
            }

            if (busBuilder.MessagingOptions.EnableSaga)
            {
                var assemblies = busBuilder.SagaOptions.ScanAssemblies;

                if (assemblies == null || assemblies.Length == 0)
                {
                    assemblies = [.. AppDomain.CurrentDomain
                        .GetAssemblies()
                        .Where(a =>
                            !a.IsDynamic &&
                            !string.IsNullOrWhiteSpace(a.FullName) &&
                            !a.FullName.StartsWith("Microsoft") &&
                            !a.FullName.StartsWith("System"))];

                    Console.WriteLine($"[Nuntium] Saga auto-scan ativado ({assemblies.Length} assemblies).");
                }
                else
                {
                    Console.WriteLine($"[Nuntium] Saga scan configurado manualmente ({assemblies.Length} assemblies).");
                }

                services.AddSagaHandlers(assemblies);

                var sagaTypes = assemblies
                    .SelectMany(a => a.GetTypes())
                    .Where(t => t.GetInterfaces()
                        .Any(i => i.IsGenericType &&
                                  i.GetGenericTypeDefinition() == typeof(ISagaHandler<,>)))
                    .ToList();

                Console.WriteLine($"[Nuntium] Saga handlers encontrados: {sagaTypes.Count}");

                foreach (var saga in sagaTypes)
                {
                    Console.WriteLine($"[Nuntium] Saga handler: {saga.FullName}");
                }
            }

            return services;
        }
    }
}