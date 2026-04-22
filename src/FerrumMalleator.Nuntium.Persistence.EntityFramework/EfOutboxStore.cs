using FerrumMalleator.Nuntium.Abstractions.Persistence;
using FerrumMalleator.Nuntium.Outbox;
using FerrumMalleator.Nuntium.Persistence.EntityFramework.Context;
using Microsoft.EntityFrameworkCore;

namespace FerrumMalleator.Nuntium.Persistence.EntityFramework
{
    internal sealed class EfOutboxStore(NuntiumDbContext db) : IOutboxStore
    {
        private readonly NuntiumDbContext _db = db;

        public async Task AddAsync(OutboxMessage message, CancellationToken ct)
        {
            _db.Set<OutboxMessage>().Add(message);
            await _db.SaveChangesAsync(ct);
        }

        public async Task<IReadOnlyList<OutboxMessage>> GetPendingAsync(int take, CancellationToken ct)
        {
            return await _db.Set<OutboxMessage>()
                .Where(x => x.ProcessedOn == null)
                .Take(take)
                .ToListAsync(ct);
        }

        public async Task MarkProcessedAsync(Guid id, CancellationToken ct)
        {
            var msg = await _db.Set<OutboxMessage>().FindAsync([id], ct);
            msg!.ProcessedOn = DateTime.UtcNow;
            await _db.SaveChangesAsync(ct);
        }

        public async Task MarkFailedAsync(Guid id, string error, CancellationToken ct)
        {
            var msg = await _db.Set<OutboxMessage>().FindAsync([id], ct);
            msg!.Error = error;
            await _db.SaveChangesAsync(ct);
        }
    }
}
