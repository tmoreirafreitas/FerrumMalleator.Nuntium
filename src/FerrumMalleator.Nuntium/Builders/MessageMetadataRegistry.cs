namespace FerrumMalleator.Nuntium.Builders
{
    public sealed class MessageMetadataRegistry
    {
        private readonly Dictionary<Type, MessageMetadata> _byType = [];
        private readonly Dictionary<string, MessageMetadata> _byKey = [];

        public void Register<T>(string topic, string groupId, Func<object, string>? partitionKey = null)
        {
            if (_byType.ContainsKey(typeof(T)))
                throw new InvalidOperationException();

            var metadata = new MessageMetadata(
                typeof(T),
                typeof(T).Name,
                topic,
                groupId,
                partitionKey);

            _byType[typeof(T)] = metadata;
            _byKey[metadata.Key] = metadata;
        }

        public MessageMetadata Get<T>() => _byType[typeof(T)];
        public MessageMetadata Get(Type type) => _byType[type];
        public MessageMetadata Get(string key) => _byKey[key];
        public bool Contains<T>() => _byKey.Any(s => s.Key == typeof(T).Name);
        public IEnumerable<MessageMetadata> GetAll()
        {
            return _byType.Values;
        }
    }
}
