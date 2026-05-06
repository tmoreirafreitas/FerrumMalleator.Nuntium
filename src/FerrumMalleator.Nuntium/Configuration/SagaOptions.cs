using System.Diagnostics.CodeAnalysis;
using System.Reflection;

namespace FerrumMalleator.Nuntium.Configuration
{
    /// <summary>
    /// Configuration options for Saga registration.
    /// </summary>
    /// <remarks>
    /// Defines how Saga handlers are discovered and registered.
    /// </remarks>
    [ExcludeFromCodeCoverage]
    public sealed class SagaOptions
    {
        /// <summary>
        /// Gets or sets the assemblies used for Saga handler scanning.
        /// </summary>
        /// <remarks>
        /// When specified, the framework will scan these assemblies
        /// to automatically register Saga handlers.
        /// </remarks>
        public Assembly[]? ScanAssemblies { get; set; }
    }
}
