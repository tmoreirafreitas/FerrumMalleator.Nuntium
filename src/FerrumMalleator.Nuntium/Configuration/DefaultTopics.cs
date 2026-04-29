using System.Diagnostics.CodeAnalysis;

namespace FerrumMalleator.Nuntium.Configuration
{
    [ExcludeFromCodeCoverage]
    internal static class DefaultTopics
    {
        public const string DeadLetterTopic = "nuntium.deadletter";
        public const string DeadLetterGroup = "nuntium.deadletter";
    }
}
