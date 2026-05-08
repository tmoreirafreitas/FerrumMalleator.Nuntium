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
using FerrumMalleator.Nuntium.Transport.Default;
using FerrumMalleator.Nuntium.Transport.InMemory;
using FerrumMalleator.Nuntium.Transport.Kafka;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;

namespace FerrumMalleator.Nuntium.Builders
{
    public static class NuntiumMessagingExtensions
    {
        /// <summary>
        /// Registers and configures the Nuntium messaging pipeline.
        /// </summary>
        /// <param name="configure">Configuration delegate for the messaging bus.</param>
        /// <returns>The updated service collection.</returns>
        /// <remarks>
        /// This is the entry point for configuring messaging features such as transport,
        /// retry policies, outbox, sagas and consumers.
        /// </remarks>
        /// <example>
        /// builder.Services.AddNuntium(bus =>
        /// {
        ///     bus.UseKafka(...)
        ///        .UseOutbox()
        ///        .AddConsumer&lt;PedidoConsumer, PedidoCriado&gt;();
        /// });
        /// </example>
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

        /// <summary>
        /// Configures retry behavior for message processing.
        /// </summary>
        /// <param name="configure">Retry policy configuration.</param>
        /// <returns>The bus builder instance.</returns>
        /// <remarks>
        /// Defines how failed message processing should be retried,
        /// including delay strategy and number of attempts.
        /// </remarks>
        public static NuntiumBusBuilder UseRetry(this NuntiumBusBuilder builder, Action<RetryPolicyOptions> configure)
        {
            configure(builder.RetryPolicy);
            builder.Services.AddSingleton(builder.RetryPolicy);
            builder.Services.AddSingleton<IRetryExecutor, RetryExecutor>();
            return builder;
        }

        /// <summary>
        /// Enables Saga support for long-running workflows.
        /// </summary>
        /// <returns>The bus builder instance.</returns>
        /// <remarks>
        /// Allows coordination of distributed processes using stateful handlers.
        /// </remarks>
        public static NuntiumBusBuilder AddSaga(this NuntiumBusBuilder builder, Action<SagaOptions>? configure = null)
        {
            builder.MessagingOptions.EnableSaga = true;

            configure?.Invoke(builder.SagaOptions);

            builder.Services.AddSingleton<SagaHandlerRegistry>();
            builder.Services.AddScoped<SagaDispatcher>();

            return builder;
        }

        /// <summary>
        /// Enables the Outbox pattern to guarantee reliable message delivery.
        /// </summary>
        /// <returns>The bus builder instance.</returns>
        /// <remarks>
        /// Messages are first persisted and later dispatched asynchronously,
        /// ensuring consistency between database operations and message publishing.
        /// Highly recommended for production environments.
        /// </remarks>
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

        /// <summary>
        /// Configures Kafka as the message transport.
        /// </summary>
        /// <param name="configure">Kafka configuration options.</param>
        /// <returns>The bus builder instance.</returns>
        /// <remarks>
        /// Enables publishing and consuming messages through Kafka.
        /// This is recommended for production scenarios.
        /// </remarks>
        public static NuntiumBusBuilder UseKafka(this NuntiumBusBuilder builder, Action<KafkaOptions> configure)
        {
            builder.Services.Configure(configure);

            builder.Services.AddSingleton<IKafkaProducerFactory, KafkaProducerFactory>();

            builder.Services.AddSingleton<IMessageTransport, KafkaTransport>();

            builder.Services.AddSingleton<IMessagePublisher, KafkaPublisher>();

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

            services.TryAddSingleton<IMessagePublisher>(sp =>
            {
                var options = busBuilder.MessagingOptions;

                if (options.EnableOutbox)
                {
                    return ActivatorUtilities.CreateInstance<OutboxPublisher>(sp);
                }

                return ActivatorUtilities.CreateInstance<DefaultMessagePublisher>(sp);
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