using FerrumMalleator.Nuntium.Sagas.Handlers;
using Microsoft.Extensions.DependencyInjection;
using System.Reflection;

namespace FerrumMalleator.Nuntium.Builders
{
    internal static class SagaRegistrationExtensions
    {
        public static IServiceCollection AddSagaHandlers(this IServiceCollection services, params Assembly[] assemblies)
        {
            var types = assemblies.SelectMany(a => a.GetTypes());
            var registry = new SagaHandlerRegistry();

            foreach (var type in types)
            {
                var interfaces = type.GetInterfaces()
                    .Where(i => i.IsGenericType &&
                                i.GetGenericTypeDefinition() == typeof(ISagaHandler<,>));

                foreach (var i in interfaces)
                {
                    var messageType = i.GetGenericArguments()[0];
                    var stateType = i.GetGenericArguments()[1];

                    var method = typeof(SagaHandlerRegistry)
                        .GetMethod(nameof(SagaHandlerRegistry.Register))?
                        .MakeGenericMethod(messageType, stateType);

                    method?.Invoke(registry, null);

                    services.AddScoped(i, type);
                }
            }

            services.AddSingleton(registry);

            return services;
        }

        public static IServiceCollection AddSagaHandlersFromAssemblyContaining<T>(this IServiceCollection services)
        {
            return services.AddSagaHandlers(typeof(T).Assembly);
        }
    }
}