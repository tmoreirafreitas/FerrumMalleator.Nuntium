namespace FerrumMalleator.Nuntium.Abstractions.Publishers
{
    /// <summary>
    /// Publishes messages to the messaging infrastructure.
    /// </summary>
    /// <remarks>
    /// Responsible for dispatching application messages through the configured transport.
    /// </remarks>
    public interface IMessagePublisher
    {
        /// <summary>
        /// Publishes a message asynchronously.
        /// </summary>
        /// <typeparam name="T">Message type.</typeparam>
        /// <param name="message">Message instance.</param>
        /// <param name="ct">Cancellation token.</param>
        /// <returns>A task representing the asynchronous operation.</returns>
        Task PublishAsync<T>(T message, CancellationToken ct = default);
    }
}
