using FerrumMalleator.Nuntium.Abstractions.Persistence;
using FerrumMalleator.Nuntium.Abstractions.Publishers;
using FerrumMalleator.Nuntium.DeadLetter;
using FerrumMalleator.Nuntium.Messaging.Models;
using FerrumMalleator.Nuntium.Persistence.InMemory;
using FerrumMalleator.Nuntium.Tests.Fake;
using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Moq;

namespace FerrumMalleator.Nuntium.Tests.UnitTests.DLQ
{
    public class DeadLetterReprocessorTests
    {
        [Fact]
        public async Task Should_reprocess_message()
        {
            var services = new ServiceCollection();

            services.AddSingleton<IDeadLetterStore, InMemoryDeadLetterStore>();

            var transporte = new FakeTransport { Throw = true };
            services.AddSingleton<IMessageTransport>(transporte);

            var provider = services.BuildServiceProvider();
            using var scope = provider.CreateScope();

            var store = scope.ServiceProvider.GetRequiredService<IDeadLetterStore>();

            var message = new DeadLetterMessage
            {
                MessageId = Guid.NewGuid(),
                MessageType = "test",
                PayloadJson = "{}",
                ReprocessCount = 0
            };

            await store.AddAsync(message, CancellationToken.None);

            var deadLetterReprocessor = new DeadLetterReprocessor(provider);

            await deadLetterReprocessor.ProcessOnceAsync(CancellationToken.None);

            transporte.Throw.Should().BeTrue();
        }

        [Fact]
        public async Task Should_schedule_retry_when_reprocess_fails()
        {
            var services = new ServiceCollection();

            services.AddSingleton<IDeadLetterStore, InMemoryDeadLetterStore>();

            var transport = new FakeTransport
            {
                Throw = true
            };

            services.AddSingleton<IMessageTransport>(transport);

            var provider = services.BuildServiceProvider();

            using var scope = provider.CreateScope();

            var store = scope.ServiceProvider.GetRequiredService<IDeadLetterStore>();

            var message = new DeadLetterMessage
            {
                MessageId = Guid.NewGuid(),
                MessageType = "test",
                PayloadJson = "{}",
                ReprocessCount = 0
            };

            await store.AddAsync(message, CancellationToken.None);

            var reprocessor = new DeadLetterReprocessor(provider);

            await reprocessor.ProcessOnceAsync(CancellationToken.None);

            var pending = await store.GetPendingAsync(10, CancellationToken.None);

            var dlq = pending.Single();

            dlq.ReprocessCount.Should().Be(1);

            dlq.NextRetryAt.Should().NotBeNull();

            dlq.NextRetryAt.Should().BeAfter(DateTime.UtcNow);
        }

        [Fact]
        public async Task Should_mark_as_reprocessed_when_max_retry_reached()
        {
            var services = new ServiceCollection();

            services.AddSingleton<IDeadLetterStore, InMemoryDeadLetterStore>();

            var transport = new FakeTransport
            {
                Throw = true
            };

            services.AddSingleton<IMessageTransport>(transport);

            var provider = services.BuildServiceProvider();

            using var scope = provider.CreateScope();

            var store = scope.ServiceProvider.GetRequiredService<IDeadLetterStore>();

            var message = new DeadLetterMessage
            {
                MessageId = Guid.NewGuid(),
                MessageType = "test",
                PayloadJson = "{}",
                ReprocessCount = 4
            };

            await store.AddAsync(message, CancellationToken.None);

            var reprocessor = new DeadLetterReprocessor(provider);

            await reprocessor.ProcessOnceAsync(CancellationToken.None);

            var pending = await store.GetPendingAsync(10, CancellationToken.None);

            pending.Should().BeEmpty();
        }

        [Fact]
        public async Task Should_ignore_delayed_retry()
        {
            var services = new ServiceCollection();

            services.AddSingleton<IDeadLetterStore, InMemoryDeadLetterStore>();

            var transport = new FakeTransport();

            services.AddSingleton<IMessageTransport>(transport);

            var provider = services.BuildServiceProvider();

            using var scope = provider.CreateScope();

            var store = scope.ServiceProvider.GetRequiredService<IDeadLetterStore>();

            var message = new DeadLetterMessage
            {
                MessageId = Guid.NewGuid(),
                MessageType = "test",
                PayloadJson = "{}",
                NextRetryAt = DateTime.UtcNow.AddMinutes(5)
            };

            await store.AddAsync(message, CancellationToken.None);

            var reprocessor = new DeadLetterReprocessor(provider);

            await reprocessor.ProcessOnceAsync(CancellationToken.None);

            transport.Sent.Should().BeFalse();
        }

        [Fact]
        public async Task Should_mark_message_as_reprocessed()
        {
            var services = new ServiceCollection();

            services.AddSingleton<IDeadLetterStore, InMemoryDeadLetterStore>();

            var transport = new FakeTransport
            {
                Throw = false
            };

            services.AddSingleton<IMessageTransport>(transport);

            var provider = services.BuildServiceProvider();

            using var scope = provider.CreateScope();

            var store = scope.ServiceProvider.GetRequiredService<IDeadLetterStore>();

            var message = new DeadLetterMessage
            {
                MessageId = Guid.NewGuid(),
                MessageType = "test",
                PayloadJson = "{}",
                ReprocessCount = 0
            };

            await store.AddAsync(message, CancellationToken.None);

            var reprocessor = new DeadLetterReprocessor(provider);

            await reprocessor.ProcessOnceAsync(CancellationToken.None);

            var pending = await store.GetPendingAsync(10, CancellationToken.None);

            pending.Should().BeEmpty();
        }

        [Fact]
        public async Task Should_throw_when_store_fails()
        {
            var services = new ServiceCollection();

            var store = new Mock<IDeadLetterStore>();

            store.Setup(x =>
                    x.GetPendingAsync(
                        It.IsAny<int>(),
                        It.IsAny<CancellationToken>()))
                .ThrowsAsync(new Exception("store failed"));

            services.AddSingleton(store.Object);

            services.AddSingleton<IMessageTransport>(new FakeTransport());

            var provider = services.BuildServiceProvider();

            var reprocessor = new DeadLetterReprocessor(provider);

            var act = async () => await reprocessor.ProcessOnceAsync(CancellationToken.None);

            await act.Should().ThrowAsync<Exception>().WithMessage("store failed");
        }

        [Fact]
        public async Task Should_skip_message_when_retry_time_not_reached()
        {
            var services = new ServiceCollection();

            services.AddSingleton<IDeadLetterStore, InMemoryDeadLetterStore>();

            services.AddSingleton<IMessageTransport, FakeTransport>();

            var provider = services.BuildServiceProvider();

            using var scope = provider.CreateScope();

            var store = scope.ServiceProvider.GetRequiredService<IDeadLetterStore>();

            await store.AddAsync(new DeadLetterMessage
            {
                MessageId = Guid.NewGuid(),
                MessageType = "test",
                PayloadJson = "{}",
                NextRetryAt = DateTime.UtcNow.AddMinutes(10)
            },
                CancellationToken.None);

            var processor = new DeadLetterReprocessor(provider);

            await processor.ProcessOnceAsync(CancellationToken.None);

            var pending = await store.GetPendingAsync(10, CancellationToken.None);

            pending.Should().HaveCount(1);
        }
    }
}
