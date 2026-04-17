using FerrumMalleator.Nuntium.Abstractions.Consumers;
using FerrumMalleator.Nuntium.Configuration;
using FerrumMalleator.Nuntium.Dispatching;
using Microsoft.Extensions.DependencyInjection;

namespace FerrumMalleator.Nuntium.Builders
{
    public sealed class NuntiumBusBuilder(IServiceCollection services)
    {
        internal IList<(Type Implementation, Type Service)> Consumers { get; } = [];
        internal MessageMetadataRegistry Registry { get; } = new();
        internal RetryPolicyOptions RetryPolicy { get; } = new();
        internal ConsumerInvokerRegistry ConsumerInvokerRegistry { get; } = new();
        public MessagingOptions MessagingOptions { get; } = new();
        internal SagaOptions SagaOptions { get; } = new();
        public IServiceCollection Services { get; } = services;

        public NuntiumBusBuilder AddConsumer<TConsumer, TMessage>()
            where TConsumer : class, IMessageConsumer<TMessage>
            where TMessage : class
        {
            Consumers.Add((typeof(TConsumer), typeof(IMessageConsumer<TMessage>)));
            ConsumerInvokerRegistry.Register<TMessage>();
            return this;
        }

        public NuntiumBusBuilder WithTopic<TMessage>(string topic, string groupId, Func<TMessage, string>? partitionKey = null) where TMessage : class
        {
            Registry.Register<TMessage>(topic, groupId, partitionKey is null ? null : msg => partitionKey((TMessage)msg));

            return this;
        }
    }
}
