using FerrumMalleator.Nuntium.Builders;
using FerrumMalleator.Nuntium.Resolution;
using FluentAssertions;
using Xunit.Sdk;

namespace FerrumMalleator.Nuntium.Tests.UnitTests.Resolution
{
    public class TopicResolverTests
    {
        [Fact]
        public void Should_resolve_topic()
        {
            var registry = new MessageMetadataRegistry();
            registry.Register<TestMessage>("test", "group");

            var resolver = new TopicResolver(registry);

            var topic = resolver?.Resolve(typeof(TestMessage));

            topic.Should().NotBeNull();
        }
    }
}
