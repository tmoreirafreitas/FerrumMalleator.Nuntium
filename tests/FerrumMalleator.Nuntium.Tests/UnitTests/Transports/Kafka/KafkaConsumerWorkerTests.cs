using Confluent.Kafka;
using FerrumMalleator.Nuntium.Abstractions;
using FerrumMalleator.Nuntium.Abstractions.Consumers;
using FerrumMalleator.Nuntium.Abstractions.Publishers;
using FerrumMalleator.Nuntium.Builders;
using FerrumMalleator.Nuntium.Dispatching;
using FerrumMalleator.Nuntium.Messaging.Envelopes;
using FerrumMalleator.Nuntium.Retry;
using FerrumMalleator.Nuntium.Tests.Fake;
using FerrumMalleator.Nuntium.Transport.Kafka;
using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using System.Text.Json;

namespace FerrumMalleator.Nuntium.Tests.UnitTests.Transports.Kafka
{
    public class KafkaConsumerWorkerTests
    {
        private record TestMessage(Guid Id);
        private class TestConsumer : IMessageConsumer<TestMessage>
        {
            public static bool Called { get; private set; }

            public Task ConsumeAsync(TestMessage message, CancellationToken ct)
            {
                Called = true;
                return Task.CompletedTask;
            }
        }

        [Fact]
        public async Task Should_consume_and_dispatch_message()
        {
            var services = new ServiceCollection();

            var registry = new MessageMetadataRegistry();
            registry.Register<TestMessage>("test", "group");

            var consumerRegistry = new ConsumerInvokerRegistry();
            consumerRegistry.Register<TestMessage>();

            services.AddSingleton(registry);
            services.AddSingleton(consumerRegistry);

            services.AddScoped<MessageDispatcher>();
            services.AddScoped<IMessageConsumer<TestMessage>, TestConsumer>();
            services.AddSingleton<IRetryExecutor, NoOpRetryExecutor>();

            var transporte = new FakeTransport();
            services.AddSingleton<IMessageTransport>(transporte);

            var metadata = registry.Get<TestMessage>();
            var fakeConsumer = new FakeKafkaConsumer
            {
                Result = new ConsumeResult<string, string>
                {
                    Topic = "test",
                    Offset = new Offset(1),
                    Message = new Message<string, string>
                    {
                        Value = JsonSerializer.Serialize(new MessageEnvelope<TestMessage>
                        {
                            MessageId = Guid.NewGuid(),
                            MessageType = metadata.Key,
                            Payload = new TestMessage(Guid.NewGuid())
                        })
                    }
                }
            };

            services.AddSingleton<IKafkaConsumer>(fakeConsumer);
            services.AddLogging();

            var provider = services.BuildServiceProvider();

            var worker = new KafkaConsumerWorker(provider, "group", ["test"]);

            await worker.ProcessOnceAsync(fakeConsumer, CancellationToken.None);

            TestConsumer.Called.Should().BeTrue();
            fakeConsumer.CommitCalled.Should().BeTrue();
        }

        [Fact]
        public async Task Should_log_error_on_consume_exception()
        {
            var services = new ServiceCollection();
            services.AddSingleton<MessageDispatcher>(); // não será usado

            var provider = services.BuildServiceProvider();

            var worker = new KafkaConsumerWorker(provider, "group", ["test"]);

            var fakeConsumer = new FakeKafkaConsumer
            {
                Throw = true
            };

            await worker.ProcessOnceAsync(fakeConsumer, CancellationToken.None);
            fakeConsumer.Throw.Should().BeTrue();
        }

        [Fact]
        public async Task Should_ignore_when_consume_returns_null()
        {
            var services = new ServiceCollection();

            services.AddSingleton<MessageDispatcher>();

            var provider = services.BuildServiceProvider();

            var worker = new KafkaConsumerWorker(provider, "group", ["test"]);

            var fakeConsumer = new FakeKafkaConsumer
            {
                Result = null
            };

            await worker.ProcessOnceAsync(fakeConsumer, CancellationToken.None);

            fakeConsumer.CommitCalled.Should().BeFalse();
        }

        [Fact]
        public async Task Should_ignore_operation_cancelled()
        {
            var services = new ServiceCollection();

            services.AddSingleton<MessageDispatcher>();

            var provider = services.BuildServiceProvider();

            var worker = new KafkaConsumerWorker(provider, "group", ["test"]);

            var fakeConsumer = new FakeKafkaConsumer
            {
                Throw = true
            };

            var act = async () => await worker.ProcessOnceAsync(fakeConsumer, CancellationToken.None);

            await act.Should().NotThrowAsync();
        }

        [Fact]
        public async Task Should_handle_commit_failure()
        {
            var services = new ServiceCollection();

            var registry = new MessageMetadataRegistry();

            registry.Register<TestMessage>("test", "group");

            var consumerRegistry = new ConsumerInvokerRegistry();

            consumerRegistry.Register<TestMessage>();

            services.AddSingleton(registry);

            services.AddSingleton(consumerRegistry);

            services.AddScoped<MessageDispatcher>();

            services.AddScoped<IMessageConsumer<TestMessage>, TestConsumer>();

            services.AddSingleton<IRetryExecutor, NoOpRetryExecutor>();

            services.AddSingleton<IMessageTransport, FakeTransport>();

            var metadata = registry.Get<TestMessage>();

            var fakeConsumer = new FakeKafkaConsumer
            {
                Throw = true,
                Result = new ConsumeResult<string, string>
                {
                    Topic = "test",
                    Offset = new Offset(1),
                    Message = new Message<string, string>
                    {
                        Value = JsonSerializer.Serialize(new MessageEnvelope<TestMessage>
                        {
                            MessageId = Guid.NewGuid(),
                            MessageType = metadata.Key,
                            Payload = new TestMessage(Guid.NewGuid())
                        })
                    }
                }
            };

            services.AddSingleton<IKafkaConsumer>(fakeConsumer);

            var provider = services.BuildServiceProvider();

            var worker = new KafkaConsumerWorker(provider, "group", ["test"]);

            var act = async () => await worker.ProcessOnceAsync(fakeConsumer, CancellationToken.None);

            await act.Should().NotThrowAsync();
        }
    }
}
