namespace FerrumMalleator.Nuntium.Abstractions.Consumers
{
    /// <summary>
    /// Consumes messages of a specific type.
    /// </summary>
    /// <typeparam name="TMessage">Message type.</typeparam>
    /// <remarks>
    /// Consumers are responsible for handling incoming messages
    /// from the configured transport.
    /// </remarks>
    public interface IMessageConsumer<in TMessage>
    {
        /// <summary>
        /// Processes an incoming message asynchronously.
        /// </summary>
        /// <param name="message">Message instance.</param>
        /// <param name="ct">Cancellation token.</param>
        /// <returns>A task representing the asynchronous operation.</returns>
        Task ConsumeAsync(TMessage message, CancellationToken ct);
    }
}
