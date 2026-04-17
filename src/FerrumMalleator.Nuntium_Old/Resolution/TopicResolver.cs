using FerrumMalleator.Nuntium.Abstractions;
using FerrumMalleator.Nuntium.Builders;
using System;

namespace FerrumMalleator.Nuntium.Resolution
{
    public class TopicResolver : ITopicResolver
    {
        private readonly MessageMetadataRegistry _registry;
        public TopicResolver(MessageMetadataRegistry registry)
        {
            _registry = registry;
        }       

        public string Resolve<T>()
        {
            return Resolve(typeof(T));
        }

        public string Resolve(Type messageType)
        {
            var metadata = _registry.Get(messageType);

            return metadata.Topic;
        }
    }
}