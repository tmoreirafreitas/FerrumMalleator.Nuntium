using System;

namespace FerrumMalleator.Nuntium.Configuration
{
    public class TopicSubscription
    {
        public Type MessageType { get; set; }
        public string Topic { get; set; }
        public string GroupId { get; set; }
    }
}
