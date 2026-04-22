using FerrumMalleator.Nuntium.Abstractions;

namespace FerrumMalleator.Nuntium.Retry
{
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
