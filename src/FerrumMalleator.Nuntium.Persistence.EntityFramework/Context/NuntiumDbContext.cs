using FerrumMalleator.Nuntium.Messaging.Models;
using FerrumMalleator.Nuntium.Outbox;
using Microsoft.EntityFrameworkCore;

namespace FerrumMalleator.Nuntium.Persistence.EntityFramework.Context
{
    public abstract class NuntiumDbContext(DbContextOptions options) : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<ProcessedMessage>().HasKey(x => x.MessageId);
            modelBuilder.Entity<ProcessedMessage>().Property(x => x.MessageId).ValueGeneratedNever();
            modelBuilder.Entity<DeadLetterMessage>(e =>
            {
                e.HasKey(x => x.MessageId);
                e.Property(x => x.MessageType).IsRequired();
                e.Property(x => x.PayloadJson).IsRequired();
                e.Property(x => x.Error).IsRequired();
                e.Property(x => x.FailedAt).IsRequired();
            });

            modelBuilder.Entity<OutboxMessage>(e =>
            {
                e.HasKey(x => x.Id);
                e.Property(x => x.Id).ValueGeneratedNever();
                e.Property(x => x.Type).IsRequired();
                e.Property(x => x.Payload).IsRequired();
                e.Property(x => x.OccurredOn).IsRequired();
                e.Property(x => x.ProcessedOn).IsRequired(false);
                e.Property(x => x.Error).IsRequired(false);
            });

            base.OnModelCreating(modelBuilder);

            OnConfigureNuntium(modelBuilder);
        }

        protected virtual void OnConfigureNuntium(ModelBuilder modelBuilder)
        {
            // mappings internos do framework
        }
    }
}
