using FerrumMalleator.Nuntium.Abstractions.Persistence;
using FerrumMalleator.Nuntium.Messaging.Models;
using FerrumMalleator.Nuntium.Persistence.EntityFramework.Context;
using Microsoft.EntityFrameworkCore;

namespace FerrumMalleator.Nuntium.Persistence.EntityFramework
{
    internal sealed class EfIdempotencyStore(NuntiumDbContext db) : IIdempotencyStore
    {
        private readonly NuntiumDbContext _db = db;

        public async Task<bool> HasProcessedAsync(Guid messageId, CancellationToken cancellationToken = default)
        {
            return await _db.Set<ProcessedMessage>()
                .AnyAsync(
                    x => x.MessageId == messageId,
                    cancellationToken);
        }

        public async Task MarkProcessedAsync(Guid messageId, CancellationToken cancellationToken = default)
        {
            try
            {
                _db.Add(new ProcessedMessage(messageId));

                await _db.SaveChangesAsync(cancellationToken);
            }
            catch (DbUpdateException)
            {
                // Inserção concorrente / duplicada.
            }
        }
    }
}
