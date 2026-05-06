using FerrumMalleator.Nuntium.Abstractions;

namespace FerrumMalleator.Nuntium.Sagas.Handlers
{
    /// <summary>
    /// Handles Saga messages and coordinates long-running workflows.
    /// </summary>
    /// <typeparam name="TMessage">Message type.</typeparam>
    /// <typeparam name="TState">Saga state type.</typeparam>
    /// <remarks>
    /// Saga handlers are responsible for orchestrating distributed processes
    /// while maintaining state consistency.
    /// </remarks>
    public interface ISagaHandler<in TMessage, in TState> where TState : ISagaState
    {
        /// <summary>
        /// Handles a Saga message asynchronously.
        /// </summary>
        /// <param name="message">Message instance.</param>
        /// <param name="state">Saga state instance.</param>
        /// <param name="stoppingToken">Cancellation token.</param>
        /// <returns>A task representing the asynchronous operation.</returns>
        Task HandleAsync(TMessage message, TState state, CancellationToken stoppingToken);
    }
}
