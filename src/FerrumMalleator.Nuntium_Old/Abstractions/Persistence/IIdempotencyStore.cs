using System;
using System.Threading;
using System.Threading.Tasks;

namespace FerrumMalleator.Nuntium.Abstractions.Persistence
{
    public interface IIdempotencyStore
    {
        Task<bool> HasProcessedAsync(Guid messageId, CancellationToken cancellationToken = default);
        Task MarkProcessedAsync(Guid messageId, CancellationToken cancellationToken = default);
    }
}
