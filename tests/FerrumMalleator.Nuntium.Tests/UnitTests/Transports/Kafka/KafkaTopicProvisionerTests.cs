using Confluent.Kafka.Admin;
using FerrumMalleator.Nuntium.Builders;
using FerrumMalleator.Nuntium.Configuration;
using FerrumMalleator.Nuntium.Tests.Fake;
using FerrumMalleator.Nuntium.Transport.Kafka;
using FluentAssertions;
using Microsoft.Extensions.Options;

namespace FerrumMalleator.Nuntium.Tests.UnitTests.Transports.Kafka
{
    public class KafkaTopicProvisionerTests
    {
        private record TestMessage;

        //private sealed class TestKafkaTopicProvisioner : KafkaTopicProvisioner
        //{
        //    public TestKafkaTopicProvisioner(
        //        IKafkaAdminClient admin,
        //        IOptions<KafkaOptions> options,
        //        MessageMetadataRegistry registry)
        //        : base(admin, options, registry)
        //    {
        //    }

        //    public Task ExecutePublicAsync(
        //        CancellationToken ct)
        //    {
        //        return ExecuteAsync(ct);
        //    }
        //}

        [Fact]
        public async Task Should_create_topics()
        {
            var registry = new MessageMetadataRegistry();
            registry.Register<TestMessage>("topic1", "group");

            var admin = new FakeKafkaAdminClient();

            var provisioner = new KafkaTopicProvisioner(
                admin,
                Options.Create(new KafkaOptions
                {
                    DefaultNumPartitions = 1,
                    DefaultReplicationFactor = 1
                }),
                registry);

            await provisioner.ProvisionAsync(CancellationToken.None);

            admin.CreatedTopics.Should().ContainSingle();
            admin.CreatedTopics[0].Name.Should().Be("topic1");
        }

        [Fact]
        public async Task Should_ignore_topic_already_exists()
        {
            var registry = new MessageMetadataRegistry();

            registry.Register<TestMessage>("topic1", "group");

            var admin = new FakeKafkaAdminClient();

            var provisioner = new KafkaTopicProvisioner(admin, Options.Create(new KafkaOptions()), registry);

            await provisioner.ProvisionAsync(CancellationToken.None);

            admin.CreateTopicsCalled.Should().BeTrue();
        }


        [Fact]
        public async Task Should_throw_on_unknown_error()
        {
            var registry = new MessageMetadataRegistry();
            registry.Register<TestMessage>("topic1", "group");

            var admin = new FakeKafkaAdminClient
            {
                ThrowOtherError = true
            };

            var provisioner = new KafkaTopicProvisioner(admin, Options.Create(new KafkaOptions()), registry);

            await Assert.ThrowsAsync<CreateTopicsException>(() => provisioner.ProvisionAsync(CancellationToken.None));
        }

        [Fact]
        public async Task Should_ignore_when_no_topics_registered()
        {
            var registry = new MessageMetadataRegistry();

            var admin = new FakeKafkaAdminClient();

            var provisioner = new KafkaTopicProvisioner(admin, Options.Create(new KafkaOptions()), registry);

            await provisioner.ProvisionAsync(CancellationToken.None);

            admin.CreateTopicsCalled.Should().BeFalse();
        }

        [Fact]
        public async Task Should_ignore_when_topic_already_exists()
        {
            var registry = new MessageMetadataRegistry();

            registry.Register<TestMessage>("topic1", "group");

            var admin = new FakeKafkaAdminClient
            {
                ThrowAlreadyExists = true
            };

            var provisioner = new KafkaTopicProvisioner(admin, Options.Create(new KafkaOptions()), registry);

            var act = async () => await provisioner.ProvisionAsync(CancellationToken.None);

            await act.Should().NotThrowAsync();

            admin.CreateTopicsCalled.Should().BeTrue();
        }

        [Fact]
        public async Task Should_throw_on_generic_error()
        {
            var registry = new MessageMetadataRegistry();

            registry.Register<TestMessage>("topic1", "group");

            var admin = new FakeKafkaAdminClient
            {
                ThrowGenericError = true
            };

            var provisioner = new KafkaTopicProvisioner(admin, Options.Create(new KafkaOptions()), registry);

            var act = async () => await provisioner.ProvisionAsync(CancellationToken.None);

            await act.Should()
                .ThrowAsync<Exception>()
                .WithMessage("generic failure");
        }
    }
}
