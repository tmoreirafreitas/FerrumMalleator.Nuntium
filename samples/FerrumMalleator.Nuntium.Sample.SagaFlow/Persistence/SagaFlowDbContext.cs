using FerrumMalleator.Nuntium.Persistence.EntityFramework.Context;
using FerrumMalleator.Nuntium.Sample.SagaFlow.Sagas;
using Microsoft.EntityFrameworkCore;

namespace FerrumMalleator.Nuntium.Sample.SagaFlow.Persistence
{
    internal class SagaFlowDbContext(DbContextOptions options) : NuntiumDbContext(options)
    {
        public DbSet<PedidoSagaState> PedidoSagas { get; set; }
        protected override void OnConfigureNuntium(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<PedidoSagaState>(e =>
            {
                e.HasKey(x => x.CorrelationId);
                e.Property(x => x.CorrelationId).ValueGeneratedNever();
                e.Property(x => x.PedidoCriado).IsRequired();
                e.Property(x => x.PagamentoAprovado).IsRequired();
            });
        }
    }
}
