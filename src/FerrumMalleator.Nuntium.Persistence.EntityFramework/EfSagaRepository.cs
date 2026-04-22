using FerrumMalleator.Nuntium.Abstractions;
using FerrumMalleator.Nuntium.Abstractions.Persistence;
using FerrumMalleator.Nuntium.Persistence.EntityFramework.Context;
using Microsoft.EntityFrameworkCore;

namespace FerrumMalleator.Nuntium.Persistence.EntityFramework
{
    internal sealed class EfSagaRepository<T>(NuntiumDbContext db) : ISagaRepository<T> where T : class, ISagaState
    {
        private readonly NuntiumDbContext _db = db;

        public async Task<T> GetAsync(Guid correlationId, CancellationToken cancellation = default)
        {
            return (await _db.Set<T>().FindAsync([correlationId, cancellation], cancellation))!;
        }

        public async Task SaveAsync(T state, CancellationToken cancellation = default)
        {
            var exists = await _db.Set<T>()
                .AnyAsync(x => x.CorrelationId == state.CorrelationId, cancellation);

            if (exists)
            {
                _db.Update(state);
            }
            else
            {
                try
                {
                    await _db.AddAsync(state, cancellation);
                }
                catch (DbUpdateException)
                {
                    // concorrência: outro processo inseriu antes
                    _db.Update(state);
                }
            }

            await _db.SaveChangesAsync(cancellation);
        }
    }
}
