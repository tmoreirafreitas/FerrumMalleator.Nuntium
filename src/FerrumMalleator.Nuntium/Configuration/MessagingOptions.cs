using System.Diagnostics.CodeAnalysis;

namespace FerrumMalleator.Nuntium.Configuration
{
    /// <summary>
    /// Defines core messaging behavior and enabled features.
    /// </summary>
    /// <remarks>
    /// Controls persistence strategy and optional messaging features
    /// such as Saga and Outbox support.
    /// </remarks>
    [ExcludeFromCodeCoverage]
    public class MessagingOptions
    {
        /// <summary>
        /// Gets or sets the persistence mode used by the framework.
        /// </summary>
        /// <remarks>
        /// Defines how internal state and messaging data are stored.
        /// </remarks>
        public PersistenceMode PersistenceMode { get; set; } = PersistenceMode.InMemory;

        /// <summary>
        /// Gets or sets a value indicating whether Saga support is enabled.
        /// </summary>
        /// <remarks>
        /// Enables orchestration of long-running distributed workflows.
        /// </remarks>
        public bool EnableSaga { get; set; } = false;

        /// <summary>
        /// Gets or sets a value indicating whether the Outbox pattern is enabled.
        /// </summary>
        /// <remarks>
        /// Ensures reliable message delivery by persisting messages
        /// before dispatching them asynchronously.
        /// Recommended for production environments.
        /// </remarks>
        public bool EnableOutbox { get; set; } = false;
    }

    /// <summary>
    /// Defines the persistence strategy used by the framework.
    /// </summary>
    public enum PersistenceMode
    {
        /// <summary>
        /// Uses in-memory persistence.
        /// Recommended for development and testing scenarios.
        /// </summary>
        InMemory,

        /// <summary>
        /// Uses Entity Framework for durable persistence.
        /// Recommended for production environments.
        /// </summary>
        EntityFramework
    }
}
