namespace FerrumMalleator.Nuntium.Messaging.Models
{
    public class DeadLetterMessage
    {
        public Guid MessageId { get; set; }
        public string MessageType { get; set; } = default!;
        public string PayloadJson { get; set; } = default!;
        public string Error { get; set; } = default!;
        public string StackTrace { get; set; } = default!;
        public int RetryCount { get; set; }
        public DateTime FailedAt { get; set; } = DateTime.UtcNow;
        public bool Reprocessed { get; set; }
        public int ReprocessCount { get; set; }
        public DateTime? NextRetryAt { get; set; }
    }
}
