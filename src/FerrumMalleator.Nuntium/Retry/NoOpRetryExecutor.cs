using FerrumMalleator.Nuntium.Abstractions;
using System.Diagnostics.CodeAnalysis;

namespace FerrumMalleator.Nuntium.Retry
{
    [ExcludeFromCodeCoverage]
    internal sealed class NoOpRetryExecutor : IRetryExecutor
    {
        public async Task ExecuteAsync(Func<Task> action, Func<Exception, int, Task> onFailure)
        {
            try
            {
                await action();
            }
            catch (Exception ex)
            {
                await onFailure(ex, 1);
                throw;
            }
        }
    }
}
