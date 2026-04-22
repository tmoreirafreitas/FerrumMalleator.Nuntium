using FerrumMalleator.Nuntium.Abstractions;
using FerrumMalleator.Nuntium.Abstractions.Persistence;
using FerrumMalleator.Nuntium.Persistence.EntityFramework.Context;
using Microsoft.EntityFrameworkCore;

namespace FerrumMalleator.Nuntium.Persistence.EntityFramework
{
    internal sealed class EfSagaRepository<T>(NuntiumDbContext db) : ISagaRepository<T> where T : class, ISagaState
    {
        private readonly NuntiumDbContext _db = db;

        public async Task<T> GetAsync(Guid correlationId, CancellationToken ct = default)
        {
            return (await _db.Set<T>().FindAsync([correlationId, ct], ct))!;
        }

        public async Task SaveAsync(T state, CancellationToken ct = default)
        {
            var exists = await _db.Set<T>()
                .AnyAsync(x => x.CorrelationId == state.CorrelationId, ct);

            if (exists)
                _db.Update(state);
            else
            {
                try
                {
                    await _db.AddAsync(state, ct);
                }
                catch (DbUpdateException)
                {
                    throw;
                }
            }

            await _db.SaveChangesAsync(ct);
        }
    }
}
