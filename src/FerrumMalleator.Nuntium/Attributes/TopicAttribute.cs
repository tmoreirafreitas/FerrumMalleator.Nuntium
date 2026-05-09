namespace FerrumMalleator.Nuntium.Attributes
{
    [AttributeUsage(AttributeTargets.Class, AllowMultiple = false)]
    public sealed class TopicAttribute(string topic, string groupId) : Attribute
    {
        public string Topic { get; } = topic;
        public string GroupId { get; } = groupId;
    }
}
