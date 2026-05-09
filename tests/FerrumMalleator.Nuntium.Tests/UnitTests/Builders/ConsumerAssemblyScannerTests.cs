using FerrumMalleator.Nuntium.Abstractions.Consumers;
using FerrumMalleator.Nuntium.Attributes;
using FerrumMalleator.Nuntium.Builders;
using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;

namespace FerrumMalleator.Nuntium.Tests.UnitTests.Builders
{
    public sealed class ConsumerAssemblyScannerTests
    {
        [Fact]
        public void Deve_registrar_consumer_automaticamente()
        {
            var services = new ServiceCollection();

            var builder = new NuntiumBusBuilder(services);

            builder.ScanConsumersFromAssembly<TestConsumer>();

            builder.Consumers.Should().Contain(x => x.Implementation == typeof(TestConsumer));
        }

        [Fact]
        public void Deve_registrar_topic_via_attribute()
        {
            var services = new ServiceCollection();

            var builder = new NuntiumBusBuilder(services);

            builder.ScanConsumersFromAssembly<TestConsumer>();

            var metadata = builder.Registry
                .GetAll()
                .Single(x => x.Key == typeof(TestMessage).Name);

            metadata.Topic.Should().Be("pedido-criado");

            metadata.GroupId.Should().Be("pedido-group");
        }

        [Fact]
        public void Nao_deve_registrar_classes_invalidas()
        {
            var services = new ServiceCollection();

            var builder = new NuntiumBusBuilder(services);

            builder.ScanConsumersFromAssembly<TestConsumer>();

            builder.Consumers.Should().NotContain(x => x.Implementation == typeof(InvalidClass));
        }

        [Fact]
        public void Deve_permitir_consumer_sem_topic_attribute()
        {
            var services = new ServiceCollection();

            var builder = new NuntiumBusBuilder(services);

            builder.ScanConsumersFromAssembly<ConsumerWithoutTopic>();

            builder.Consumers.Should().Contain(x => x.Implementation == typeof(ConsumerWithoutTopic));
        }

        [Topic("pedido-criado", "pedido-group")]
        private sealed record TestMessage(Guid PedidoId);

        private sealed class TestConsumer : IMessageConsumer<TestMessage>
        {
            public Task ConsumeAsync(TestMessage message, CancellationToken ct)
            {
                return Task.CompletedTask;
            }
        }

        private sealed class ConsumerWithoutTopic : IMessageConsumer<MessageWithoutTopic>
        {
            public Task ConsumeAsync(MessageWithoutTopic message, CancellationToken ct)
            {
                return Task.CompletedTask;
            }
        }

        private sealed record MessageWithoutTopic(Guid PedidoId);
        private abstract class InvalidClass { }
    }
}
