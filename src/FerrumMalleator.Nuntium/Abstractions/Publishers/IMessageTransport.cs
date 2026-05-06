namespace FerrumMalleator.Nuntium.Abstractions.Publishers
{
    /// <summary>
    /// Defines a low-level transport responsible for sending raw messages.
    /// </summary>
    /// <remarks>
    /// Used internally by the framework to dispatch serialized payloads
    /// through the configured messaging infrastructure.
    /// </remarks>
    public interface IMessageTransport
    {
        /// <summary>
        /// Sends a serialized message asynchronously.
        /// </summary>
        /// <param name="messageType">Logical message type identifier.</param>
        /// <param name="payload">Serialized payload.</param>
        /// <param name="ct">Cancellation token.</param>
        /// <returns>A task representing the asynchronous operation.</returns>
        Task SendAsync(string messageType, string payload, CancellationToken ct);
    }
}
