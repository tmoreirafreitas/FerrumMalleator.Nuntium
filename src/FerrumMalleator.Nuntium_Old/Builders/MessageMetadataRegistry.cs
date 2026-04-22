using System;
using System.Collections.Generic;

namespace FerrumMalleator.Nuntium.Builders
{
    public sealed class MessageMetadataRegistry
    {
        private readonly Dictionary<Type, MessageMetadata> _byType = new Dictionary<Type, MessageMetadata>();
        private readonly Dictionary<string, MessageMetadata> _byKey = new Dictionary<string, MessageMetadata>();

        public void Register<T>(string topic, string groupId)
        {
            if (_byType.ContainsKey(typeof(T)))
                throw new InvalidOperationException();

            var metadata = new MessageMetadata(
                typeof(T),
                typeof(T).Name,
                topic,
                groupId);

            _byType[typeof(T)] = metadata;
            _byKey[metadata.Key] = metadata;
        }

        public MessageMetadata Get<T>() => _byType[typeof(T)];
        public MessageMetadata Get(Type type) => _byType[type];
        public MessageMetadata Get(string key) => _byKey[key];
        public IEnumerable<MessageMetadata> GetAll()
        {
            return _byType.Values;
        }
    }
}
