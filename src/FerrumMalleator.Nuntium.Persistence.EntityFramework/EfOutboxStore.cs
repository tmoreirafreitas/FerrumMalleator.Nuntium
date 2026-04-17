using FerrumMalleator.Nuntium.Abstractions.Persistence;
using FerrumMalleator.Nuntium.Outbox;
using FerrumMalleator.Nuntium.Persistence.EntityFramework.Context;
using Microsoft.EntityFrameworkCore;

namespace FerrumMalleator.Nuntium.Persistence.EntityFramework
{
    internal sealed class EfOutboxStore(NuntiumDbContext db) : IOutboxStore
    {
        private readonly NuntiumDbContext _db = db;

        public async Task AddAsync(OutboxMessage message, CancellationToken cancellationToken)
        {
            _db.Set<OutboxMessage>().Add(message);
            await _db.SaveChangesAsync(cancellationToken);
        }

        public async Task<IReadOnlyList<OutboxMessage>> GetPendingAsync(int take, CancellationToken cancellationToken)
        {
            return await _db.Set<OutboxMessage>()
                .Where(x => x.ProcessedOn == null)
                .Take(take)
                .ToListAsync(cancellationToken: cancellationToken);
        }

        public async Task MarkProcessedAsync(Guid id, CancellationToken cancellationToken)
        {
            var msg = await _db.Set<OutboxMessage>().FindAsync([id], cancellationToken);
            msg!.ProcessedOn = DateTime.UtcNow;
            await _db.SaveChangesAsync(cancellationToken);
        }

        public async Task MarkFailedAsync(Guid id, string error, CancellationToken cancellationToken)
        {
            var msg = await _db.Set<OutboxMessage>().FindAsync([id], cancellationToken: cancellationToken);
            msg!.Error = error;
            await _db.SaveChangesAsync(cancellationToken);
        }
    }
}
