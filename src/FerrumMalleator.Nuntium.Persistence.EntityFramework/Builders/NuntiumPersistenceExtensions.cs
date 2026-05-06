using FerrumMalleator.Nuntium.Abstractions.Persistence;
using FerrumMalleator.Nuntium.Builders;
using FerrumMalleator.Nuntium.Configuration;
using FerrumMalleator.Nuntium.Persistence.EntityFramework.Context;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;

namespace FerrumMalleator.Nuntium.Persistence.EntityFramework.Builders
{
    public static class NuntiumPersistenceExtensions
    {
        /// <summary>
        /// Configures persistence for messaging components such as Outbox and Saga state.
        /// </summary>
        /// <typeparam name="TContext">Database context type.</typeparam>
        /// <param name="configure">Persistence configuration.</param>
        /// <returns>The bus builder instance.</returns>
        /// <remarks>
        /// Enables durable storage using a database provider such as Entity Framework.
        /// </remarks>
        public static NuntiumBusBuilder UseEfCorePersistence<TDbContext>(this NuntiumBusBuilder builder, Action<DbContextOptionsBuilder> configure)
            where TDbContext : NuntiumDbContext
        {
            builder.MessagingOptions.PersistenceMode = PersistenceMode.EntityFramework;

            builder.Services.AddDbContext<TDbContext>(configure);

            builder.Services.AddScoped<NuntiumDbContext>(provider => provider.GetRequiredService<TDbContext>());

            builder.Services.AddScoped<IOutboxStore, EfOutboxStore>();

            builder.Services.AddScoped<IIdempotencyStore, EfIdempotencyStore>();

            builder.Services.AddScoped(typeof(ISagaRepository<>), typeof(EfSagaRepository<>));

            builder.Services.AddScoped<IDeadLetterStore, EfDeadLetterStore>();

            return builder;
        }
    }
}