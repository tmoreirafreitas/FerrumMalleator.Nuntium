namespace FerrumMalleator.Nuntium.Configuration
{
    /// <summary>
    /// Defines retry behavior for message processing.
    /// </summary>
    /// <remarks>
    /// Controls how failed operations should be retried,
    /// including retry attempts and delay intervals.
    /// </remarks>
    /// [ExcludeFromCodeCoverage]
    public sealed class RetryPolicyOptions
    {
        /// <summary>
        /// Gets or sets the maximum number of retry attempts.
        /// </summary>
        /// <remarks>
        /// Defines how many times a failed operation will be retried
        /// before being considered permanently failed.
        /// </remarks>
        public int MaxAttempts { get; set; } = 4;

        /// <summary>
        /// Gets or sets the retry delay intervals.
        /// </summary>
        /// <remarks>
        /// Each entry represents the delay applied before the next retry attempt.
        /// Delays are applied sequentially based on the retry count.
        /// </remarks>
        /// <example>
        /// <code>
        /// options.Delays =
        /// [
        ///     TimeSpan.FromSeconds(2),
        ///     TimeSpan.FromSeconds(5),
        ///     TimeSpan.FromSeconds(10)
        /// ];
        /// </code>
        /// </example>
        public IList<TimeSpan> Delays { get; set; } =
        [
            TimeSpan.FromSeconds(2),
            TimeSpan.FromSeconds(5),
            TimeSpan.FromSeconds(15),
            TimeSpan.FromSeconds(30)
        ];
    }
}
