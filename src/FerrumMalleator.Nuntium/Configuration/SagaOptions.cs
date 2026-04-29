using System.Diagnostics.CodeAnalysis;
using System.Reflection;

namespace FerrumMalleator.Nuntium.Configuration
{
    [ExcludeFromCodeCoverage]
    public sealed class SagaOptions
    {
        public Assembly[]? ScanAssemblies { get; set; }
    }
}
