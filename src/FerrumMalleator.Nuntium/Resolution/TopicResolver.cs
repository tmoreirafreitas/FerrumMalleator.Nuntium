using FerrumMalleator.Nuntium.Abstractions;
using FerrumMalleator.Nuntium.Builders;

namespace FerrumMalleator.Nuntium.Resolution
{
    internal sealed class TopicResolver(MessageMetadataRegistry registry) : ITopicResolver
    {
        private readonly MessageMetadataRegistry _registry = registry;

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