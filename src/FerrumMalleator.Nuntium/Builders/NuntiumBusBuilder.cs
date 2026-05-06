using FerrumMalleator.Nuntium.Abstractions.Consumers;
using FerrumMalleator.Nuntium.Configuration;
using FerrumMalleator.Nuntium.Dispatching;
using Microsoft.Extensions.DependencyInjection;
using System.Diagnostics.CodeAnalysis;

namespace FerrumMalleator.Nuntium.Builders
{
    [ExcludeFromCodeCoverage]
    public sealed class NuntiumBusBuilder(IServiceCollection services)
    {
        internal IList<(Type Implementation, Type Service)> Consumers { get; } = [];
        internal MessageMetadataRegistry Registry { get; } = new();
        internal RetryPolicyOptions RetryPolicy { get; } = new();
        internal ConsumerInvokerRegistry ConsumerInvokerRegistry { get; } = new();
        public MessagingOptions MessagingOptions { get; } = new();
        internal SagaOptions SagaOptions { get; } = new();
        public IServiceCollection Services { get; } = services;

        /// <summary>
        /// Registers a message consumer.
        /// </summary>
        /// <typeparam name="TConsumer">Consumer implementation.</typeparam>
        /// <typeparam name="TMessage">Message type handled by the consumer.</typeparam>
        /// <returns>The bus builder instance.</returns>
        /// <remarks>
        /// Consumers are responsible for handling incoming messages.
        /// Multiple consumers can be registered for different message types.
        /// </remarks>
        public NuntiumBusBuilder AddConsumer<TConsumer, TMessage>()
            where TConsumer : class, IMessageConsumer<TMessage>
        {
            Consumers.Add((typeof(TConsumer), typeof(IMessageConsumer<TMessage>)));
            ConsumerInvokerRegistry.Register<TMessage>();
            return this;
        }

        /// <summary>
        /// Maps a message type to a Kafka topic and consumer group.
        /// </summary>
        /// <typeparam name="TMessage">Message type.</typeparam>
        /// <param name="topic">Kafka topic name.</param>
        /// <param name="groupId">Consumer group identifier.</param>
        /// <param name="partitionKey">Optional partition key selector.</param>
        /// <returns>The bus builder instance.</returns>
        /// <remarks>
        /// Defines how messages are routed and consumed within Kafka.
        /// </remarks>
        public NuntiumBusBuilder WithTopic<TMessage>(string topic, string groupId, Func<TMessage, string>? partitionKey = null)
        {
            Registry.Register<TMessage>(topic, groupId, partitionKey is null ? null : msg => partitionKey((TMessage)msg));

            return this;
        }
    }
}
