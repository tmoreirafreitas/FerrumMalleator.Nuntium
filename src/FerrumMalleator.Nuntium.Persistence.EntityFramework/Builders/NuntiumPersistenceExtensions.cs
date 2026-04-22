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
        public static NuntiumBusBuilder UsePersistence<TDbContext>(this NuntiumBusBuilder builder, Action<DbContextOptionsBuilder> configure)
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