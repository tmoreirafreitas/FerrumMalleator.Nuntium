using FerrumMalleator.Nuntium.Persistence.EntityFramework.Context;
using Microsoft.EntityFrameworkCore;

namespace FerrumMalleator.Nuntium.Sample.OutboxFlow.Persistence
{
    internal class OutboxFlowDbContext(DbContextOptions options) : NuntiumDbContext(options)
    {

    }
}
