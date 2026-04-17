using FerrumMalleator.Nuntium.Abstractions;
using FerrumMalleator.Nuntium.Configuration;

namespace FerrumMalleator.Nuntium.Retry
{
    internal sealed class RetryExecutor(RetryPolicyOptions options) : IRetryExecutor
    {
        private readonly RetryPolicyOptions _options = options;

        public async Task ExecuteAsync(Func<Task> action, Func<Exception, int, Task> onFailure)
        {
            var delays = _options.Delays;
            var maxAttempts = _options.MaxAttempts;

            for (int attempt = 1; attempt <= maxAttempts; attempt++)
            {
                try
                {
                    await action();
                    return;
                }
                catch (Exception ex)
                {
                    if (attempt == maxAttempts)
                    {
                        await onFailure(ex, attempt);
                        throw;
                    }

                    await Task.Delay(delays[Math.Min(attempt - 1, delays.Count - 1)]);
                }
            }
        }
    }
}
