using System;
namespace FerrumMalleator.Nuntium.Messaging.Models
{
    public class DeadLetterMessage
    {
        public Guid MessageId { get; }
        public string MessageType { get; }
        public string PayloadJson { get; }
        public string Error { get; }
        public string StackTrace { get; }
        public int RetryCount { get; }
        public DateTime OccurredOn { get; }
        public DateTime FailedAt { get; }

        public DeadLetterMessage(
            Guid messageId, 
            string messageType, 
            string payloadJson, 
            string error, 
            string stackTrace, 
            int retryCount, 
            DateTime occurredOn, 
            DateTime failedAt)
        {
            MessageId = messageId;
            MessageType = messageType;
            PayloadJson = payloadJson;
            Error = error;
            StackTrace = stackTrace;
            RetryCount = retryCount;
            OccurredOn = occurredOn;
            FailedAt = failedAt;
        }        
    }
}
