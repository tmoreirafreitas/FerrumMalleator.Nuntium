namespace FerrumMalleator.Nuntium.Builders
{
    public sealed class MessageMetadata(Type type, string key, string topic, string groupId, Func<object, string>? partitionKey)
    {
        public Type Type { get; } = type;
        public string Key { get; } = key;
        public string Topic { get; } = topic;
        public string GroupId { get; } = groupId;
        public Func<object, string>? PartitionKey { get; } = partitionKey;
    }
}
