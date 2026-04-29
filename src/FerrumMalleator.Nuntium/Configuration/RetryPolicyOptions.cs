using System.Diagnostics.CodeAnalysis;

namespace FerrumMalleator.Nuntium.Configuration
{
    [ExcludeFromCodeCoverage]
    public sealed class RetryPolicyOptions
    {
        public int MaxAttempts { get; set; } = 4;

        public IList<TimeSpan> Delays { get; set; } =
        [
            TimeSpan.FromSeconds(2),
            TimeSpan.FromSeconds(5),
            TimeSpan.FromSeconds(15),
            TimeSpan.FromSeconds(30)
        ];
    }
}
