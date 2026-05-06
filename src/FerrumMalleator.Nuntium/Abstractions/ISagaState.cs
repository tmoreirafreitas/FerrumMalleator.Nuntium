namespace FerrumMalleator.Nuntium.Abstractions
{
    /// <summary>
    /// Represents Saga state persistence.
    /// </summary>
    /// <remarks>
    /// Saga states are used to maintain workflow consistency
    /// across distributed operations.
    /// </remarks>
    public interface ISagaState
    {
        /// <summary>
        /// Gets or sets the Saga correlation identifier.
        /// </summary>
        Guid CorrelationId { get; set; }
    }
}
