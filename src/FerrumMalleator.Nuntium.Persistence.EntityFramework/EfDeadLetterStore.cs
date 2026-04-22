using FerrumMalleator.Nuntium.Abstractions.Persistence;
using FerrumMalleator.Nuntium.Messaging.Models;
using FerrumMalleator.Nuntium.Persistence.EntityFramework.Context;
using Microsoft.EntityFrameworkCore;

namespace FerrumMalleator.Nuntium.Persistence.EntityFramework
{
    internal sealed class EfDeadLetterStore(NuntiumDbContext db) : IDeadLetterStore
    {
        private readonly NuntiumDbContext _db = db;

        public async Task AddAsync(DeadLetterMessage message, CancellationToken ct)
        {
            _db.Set<DeadLetterMessage>().Add(message);
            await _db.SaveChangesAsync(ct);
        }

        public async Task<IReadOnlyList<DeadLetterMessage>> GetPendingAsync(int take, CancellationToken ct)
        {
            var now = DateTime.UtcNow;

            var messages = await _db.Set<DeadLetterMessage>()
                .Where(x =>
                    !x.Reprocessed &&
                    (x.NextRetryAt == null || x.NextRetryAt <= now))
                .OrderBy(x => x.FailedAt)
                .Take(take)
                .ToListAsync(ct);

            return messages;
        }

        public async Task MarkReprocessedAsync(Guid messageId, CancellationToken ct)
        {
            var entity = await _db.Set<DeadLetterMessage>().FirstOrDefaultAsync(x => x.MessageId == messageId, ct);

            if (entity is null)
                return;

            entity.Reprocessed = true;

            await _db.SaveChangesAsync(ct);
        }

        public async Task UpdateRetryAsync(Guid messageId, int reprocessCount, DateTime nextRetryAt, CancellationToken ct)
        {
            var entity = await _db.Set<DeadLetterMessage>().FirstOrDefaultAsync(x => x.MessageId == messageId, ct);

            if (entity is null)
                return;

            entity.ReprocessCount = reprocessCount;
            entity.NextRetryAt = nextRetryAt;

            await _db.SaveChangesAsync(ct);
        }
    }
}
