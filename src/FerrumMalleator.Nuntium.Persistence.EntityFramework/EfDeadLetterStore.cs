using FerrumMalleator.Nuntium.Abstractions.Persistence;
using FerrumMalleator.Nuntium.Messaging.Models;
using FerrumMalleator.Nuntium.Persistence.EntityFramework.Context;
using Microsoft.EntityFrameworkCore;

namespace FerrumMalleator.Nuntium.Persistence.EntityFramework
{
    internal sealed class EfDeadLetterStore(NuntiumDbContext db) : IDeadLetterStore
    {
        private readonly NuntiumDbContext _db = db;

        public async Task AddAsync(DeadLetterMessage message, CancellationToken cancellation)
        {
            _db.Set<DeadLetterMessage>().Add(message);
            await _db.SaveChangesAsync(cancellation);
        }

        public async Task<IReadOnlyList<DeadLetterMessage>> GetPendingAsync(int take, CancellationToken cancellation)
        {
            var now = DateTime.UtcNow;

            var messages = await _db.Set<DeadLetterMessage>()
                .Where(x =>
                    !x.Reprocessed &&
                    (x.NextRetryAt == null || x.NextRetryAt <= now))
                .OrderBy(x => x.FailedAt)
                .Take(take)
                .ToListAsync(cancellation);

            return messages;
        }

        public async Task MarkReprocessedAsync(Guid messageId, CancellationToken cancellation)
        {
            var entity = await _db.Set<DeadLetterMessage>().FirstOrDefaultAsync(x => x.MessageId == messageId, cancellation);

            if (entity is null)
                return;

            entity.Reprocessed = true;

            await _db.SaveChangesAsync(cancellation);
        }

        public async Task UpdateRetryAsync(Guid messageId, int reprocessCount, DateTime nextRetryAt, CancellationToken cancellation)
        {
            var entity = await _db.Set<DeadLetterMessage>().FirstOrDefaultAsync(x => x.MessageId == messageId, cancellation);

            if (entity is null)
                return;

            entity.ReprocessCount = reprocessCount;
            entity.NextRetryAt = nextRetryAt;

            await _db.SaveChangesAsync(cancellation);
        }
    }
}
