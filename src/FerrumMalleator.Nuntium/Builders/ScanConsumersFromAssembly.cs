using FerrumMalleator.Nuntium.Abstractions.Consumers;
using FerrumMalleator.Nuntium.Attributes;
using System.Reflection;

namespace FerrumMalleator.Nuntium.Builders
{
    internal static class ConsumerAssemblyScanner
    {
        public static void Scan(NuntiumBusBuilder builder, Assembly assembly)
        {
            var consumers = assembly
                .GetTypes()
                .Where(x =>
                    !x.IsAbstract &&
                    !x.IsInterface)
                .SelectMany(type =>
                    type.GetInterfaces()
                        .Where(i =>
                            i.IsGenericType &&
                            i.GetGenericTypeDefinition() ==
                            typeof(IMessageConsumer<>))
                        .Select(i => new
                        {
                            Consumer = type,
                            Message = i.GetGenericArguments()[0]
                        }))
                .ToList();

            foreach (var item in consumers)
            {
                var addConsumerMethod =
                    typeof(NuntiumBusBuilder)
                        .GetMethods()
                        .First(x =>
                            x.Name == nameof(NuntiumBusBuilder.AddConsumer) &&
                            x.IsGenericMethodDefinition &&
                            x.GetGenericArguments().Length == 2);

                var genericMethod = addConsumerMethod.MakeGenericMethod(item.Consumer, item.Message);

                genericMethod.Invoke(builder, []);

                RegisterTopicIfExists(builder, item.Message);
            }
        }

        private static void RegisterTopicIfExists(NuntiumBusBuilder builder, Type messageType)
        {
            var attribute = messageType.GetCustomAttribute<TopicAttribute>();

            if (attribute is null)
            {
                return;
            }

            var withTopicMethod =
                typeof(NuntiumBusBuilder)
                    .GetMethods()
                    .First(x =>
                        x.Name == nameof(NuntiumBusBuilder.WithTopic) &&
                        x.IsGenericMethodDefinition);

            var genericMethod = withTopicMethod.MakeGenericMethod(messageType);

            genericMethod.Invoke(builder,
            [
                attribute.Topic,
                attribute.GroupId,
                null!
            ]);
        }
    }
}