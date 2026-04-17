using System;

namespace FerrumMalleator.Nuntium.Outbox
{
    public class OutboxMessage
    {
        public Guid Id { get; set; }
        public string Type { get; set; }
        public string Payload { get; set; }
        public DateTime OccurredOn { get; set; }
        public DateTime? ProcessedOn { get; set; }
        public string Error { get; set; }
    }
}
