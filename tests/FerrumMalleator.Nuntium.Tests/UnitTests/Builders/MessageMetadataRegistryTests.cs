using FerrumMalleator.Nuntium.Builders;
using FluentAssertions;

namespace FerrumMalleator.Nuntium.Tests.UnitTests.Builders;

public class MessageMetadataRegistryTests
{
    private record TestMessage;

    [Fact]
    public void Should_register_and_resolve_message()
    {
        var registry = new MessageMetadataRegistry();

        registry.Register<TestMessage>("test-topic", "test-group");

        var metadata = registry.Get<TestMessage>();

        metadata.Should().NotBeNull();
        metadata.Topic.Should().Be("test-topic");
        metadata.GroupId.Should().Be("test-group");
    }

    [Fact]
    public void Should_resolve_by_key()
    {
        var registry = new MessageMetadataRegistry();

        registry.Register<TestMessage>("test-topic", "test-group");

        var metadata = registry.Get("TestMessage");

        metadata.Type.Should().Be<TestMessage>();
    }

    [Fact]
    public void Should_throw_when_duplicate_registration()
    {
        var registry = new MessageMetadataRegistry();

        registry.Register<TestMessage>("test-topic", "test-group");

        var act = () => registry.Register<TestMessage>("test-topic", "test-group");

        act.Should().Throw<InvalidOperationException>();
    }
}