using System;

namespace FerrumMalleator.Nuntium.Builders
{
    public sealed class MessageMetadata
    {
        public Type Type { get; }
        public string Key { get; }
        public string Topic { get; }
        public string GroupId { get; }
        public MessageMetadata(Type type, string key, string topic, string groupId)
        {
            Type = type;
            Key = key;
            Topic = topic;
            GroupId = groupId;
        }
    }
}
