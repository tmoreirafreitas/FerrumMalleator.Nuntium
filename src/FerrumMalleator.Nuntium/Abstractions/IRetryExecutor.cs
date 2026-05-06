namespace FerrumMalleator.Nuntium.Abstractions
{
    /// <summary>
    /// Executes operations using retry policies.
    /// </summary>
    /// <remarks>
    /// Responsible for handling transient failures
    /// during message processing.
    /// </remarks>
    public interface IRetryExecutor
    {
        /// <summary>
        /// Executes an operation with retry support.
        /// </summary>
        /// <param name="action">Operation to execute.</param>
        /// <param name="onFailure">Failure callback.</param>
        /// <returns>A task representing the asynchronous operation.</returns>
        Task ExecuteAsync(Func<Task> action, Func<Exception, int, Task> onFailure);
    }
}
