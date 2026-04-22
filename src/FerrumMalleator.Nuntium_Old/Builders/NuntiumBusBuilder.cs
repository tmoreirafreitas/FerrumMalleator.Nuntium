using FerrumMalleator.Nuntium.Abstractions.Consumers;
using FerrumMalleator.Nuntium.Configuration;
using FerrumMalleator.Nuntium.Dispatching;
using Microsoft.Extensions.DependencyInjection;
using System;
using System.Collections.Generic;
using System.Reflection;

namespace FerrumMalleator.Nuntium.Builders
{
    public class NuntiumBusBuilder
    {
        internal IList<(Type Implementation, Type Service)> Consumers { get; } = new List<(Type Implementation, Type Service)>();
        internal IList<Assembly> ConsumerAssemblies { get; } = new List<Assembly>();
        internal IList<Assembly> SagaAssemblies { get; } = new List<Assembly>();
        internal MessageMetadataRegistry Registry { get; } = new MessageMetadataRegistry();
        internal RetryPolicyOptions RetryPolicy { get; } = new RetryPolicyOptions();
        internal ConsumerInvokerRegistry ConsumerInvokerRegistry { get; } = new ConsumerInvokerRegistry();        
        internal MessagingOptions MessagingOptions { get; } = new MessagingOptions();
        internal KafkaOptions KafkaOptions { get; } = new KafkaOptions();
        public IServiceCollection Services { get; }

        public NuntiumBusBuilder(IServiceCollection services)
        {
            Services = services;
        }

        public NuntiumBusBuilder AddConsumersFromAssembly(Assembly assembly)
        {
            ConsumerAssemblies.Add(assembly);

            return this;
        }

        public NuntiumBusBuilder AddSagasFromAssembly(Assembly assembly)
        {
            SagaAssemblies.Add(assembly);

            return this;
        }

        public NuntiumBusBuilder AddConsumersFromAssemblyContaining<T>()
        {
            return AddConsumersFromAssembly(typeof(T).Assembly);
        }

        public NuntiumBusBuilder AddSagasFromAssemblyContaining<T>()
        {
            return AddSagasFromAssembly(typeof(T).Assembly);
        }

        public NuntiumBusBuilder AddConsumer<TConsumer, TMessage>()
            where TConsumer : class, IMessageConsumer<TMessage>
            where TMessage : class
        {
            Consumers.Add((typeof(TConsumer), typeof(IMessageConsumer<TMessage>)));
            ConsumerInvokerRegistry.Register<TMessage>();
            return this;
        }

        public NuntiumBusBuilder WithTopic<TMessage>(string topic, string groupId) where TMessage : class
        {
            Registry.Register<TMessage>(topic, groupId);
            return this;
        }

        public NuntiumBusBuilder UseRetry(Action<RetryPolicyOptions> configure)
        {
            configure(RetryPolicy);

            return this;
        }
    }
}
