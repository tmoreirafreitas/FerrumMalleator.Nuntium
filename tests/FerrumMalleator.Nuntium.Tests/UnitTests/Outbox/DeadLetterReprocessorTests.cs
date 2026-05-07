using FerrumMalleator.Nuntium.Abstractions.Persistence;
using FerrumMalleator.Nuntium.Abstractions.Publishers;
using FerrumMalleator.Nuntium.DeadLetter;
using FerrumMalleator.Nuntium.Messaging.Models;
using FerrumMalleator.Nuntium.Persistence.InMemory;
using FerrumMalleator.Nuntium.Tests.Fake;
using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;

namespace FerrumMalleator.Nuntium.Tests.UnitTests.Outbox
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
    }
}
