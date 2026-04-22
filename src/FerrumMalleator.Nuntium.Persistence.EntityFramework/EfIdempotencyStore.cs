using FerrumMalleator.Nuntium.Abstractions.Persistence;
using FerrumMalleator.Nuntium.Messaging.Models;
using FerrumMalleator.Nuntium.Persistence.EntityFramework.Context;
using Microsoft.EntityFrameworkCore;

namespace FerrumMalleator.Nuntium.Persistence.EntityFramework
{
    internal sealed class EfIdempotencyStore(NuntiumDbContext db) : IIdempotencyStore
    {
        private readonly NuntiumDbContext _db = db;

        public async Task<bool> HasProcessedAsync(Guid messageId, CancellationToken cancellation = default)
        {
            return await _db.Set<ProcessedMessage>()
                .AnyAsync(
                    x => x.MessageId == messageId,
                    cancellation);
        }

        public async Task MarkProcessedAsync(Guid messageId, CancellationToken ct = default)
        {
            try
            {
                if (await _db.Set<ProcessedMessage>().AnyAsync(x => x.MessageId == messageId, ct))
                    return;

                _db.Add(new ProcessedMessage(messageId));
                await _db.SaveChangesAsync(ct);
            }
            catch (DbUpdateException)
            {
                // fallback para condição de corrida (concorrência)
                // idempotência garantida
            }
        }
    }
}
