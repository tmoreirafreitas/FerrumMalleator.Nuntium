using FerrumMalleator.Nuntium.Abstractions.Persistence;
using FerrumMalleator.Nuntium.Outbox;
using FerrumMalleator.Nuntium.Persistence.EntityFramework.Context;
using Microsoft.EntityFrameworkCore;

namespace FerrumMalleator.Nuntium.Persistence.EntityFramework
{
    internal sealed class EfOutboxStore(NuntiumDbContext db) : IOutboxStore
    {
        private readonly NuntiumDbContext _db = db;

        public async Task AddAsync(OutboxMessage message, CancellationToken cancellation)
        {
            _db.Set<OutboxMessage>().Add(message);
            await _db.SaveChangesAsync(cancellation);
        }

        public async Task<IReadOnlyList<OutboxMessage>> GetPendingAsync(int take, CancellationToken cancellation)
        {
            return await _db.Set<OutboxMessage>()
                .Where(x => x.ProcessedOn == null)
                .Take(take)
                .ToListAsync(cancellation);
        }

        public async Task MarkProcessedAsync(Guid id, CancellationToken cancellation)
        {
            var msg = await _db.Set<OutboxMessage>().FindAsync([id], cancellation);
            msg!.ProcessedOn = DateTime.UtcNow;
            await _db.SaveChangesAsync(cancellation);
        }

        public async Task MarkFailedAsync(Guid id, string error, CancellationToken cancellation)
        {
            var msg = await _db.Set<OutboxMessage>().FindAsync([id], cancellation);
            msg!.Error = error;
            await _db.SaveChangesAsync(cancellation);
        }
    }
}
