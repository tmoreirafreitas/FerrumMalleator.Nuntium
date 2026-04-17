using FerrumMalleator.Nuntium.Abstractions;
using FerrumMalleator.Nuntium.Abstractions.Persistence;
using FerrumMalleator.Nuntium.Persistence.EntityFramework.Context;
using Microsoft.EntityFrameworkCore;

namespace FerrumMalleator.Nuntium.Persistence.EntityFramework
{
    internal sealed class EfSagaRepository<T>(NuntiumDbContext db) : ISagaRepository<T> where T : class, ISagaState
    {
        private readonly NuntiumDbContext _db = db;

        public async Task<T> GetAsync(Guid correlationId, CancellationToken cancellationToken = default)
        {
            return (await _db.Set<T>().FindAsync([correlationId, cancellationToken], cancellationToken: cancellationToken))!;
        }

        public async Task SaveAsync(T state, CancellationToken cancellationToken = default)
        {
            var exists = await _db.Set<T>()
                .AnyAsync(x => x.CorrelationId == state.CorrelationId, cancellationToken);

            if (exists)
                _db.Update(state);
            else
            {
                try
                {
                    await _db.AddAsync(state, cancellationToken);
                }
                catch (DbUpdateException)
                {
                    throw;
                }
            }

            await _db.SaveChangesAsync(cancellationToken);
        }
    }
}
