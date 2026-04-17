using FerrumMalleator.Nuntium.Outbox;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace FerrumMalleator.Nuntium.Abstractions.Persistence
{
    public interface IOutboxStore
    {
        Task AddAsync(OutboxMessage message, CancellationToken cancellation);
        Task<IReadOnlyList<OutboxMessage>> GetPendingAsync(int take, CancellationToken cancellation);
        Task MarkProcessedAsync(Guid id, CancellationToken cancellation);
        Task MarkFailedAsync(Guid id, string error, CancellationToken cancellation);
    }
}
