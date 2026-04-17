namespace FerrumMalleator.Nuntium.Configuration
{
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
