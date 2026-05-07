using FerrumMalleator.Nuntium.Messaging.Models;
using FerrumMalleator.Nuntium.Persistence.EntityFramework;
using FerrumMalleator.Nuntium.Persistence.EntityFramework.Context;
using FluentAssertions;
using Microsoft.EntityFrameworkCore;

namespace FerrumMalleator.Nuntium.Tests.UnitTests.Persistence
{
    public class EfDeadLetterStoreTests
    {
        private class TestDbContext(DbContextOptions options) : NuntiumDbContext(options)
        {

        }

        private static TestDbContext CreateDb()
        {
            var options = new DbContextOptionsBuilder<TestDbContext>()
                .UseInMemoryDatabase(Guid.NewGuid().ToString())
                .Options;

            return new TestDbContext(options);
        }

        [Fact]
        public async Task Should_add_deadletter_message()
        {
            var db = CreateDb();
            var store = new EfDeadLetterStore(db);

            var message = CreateValidMessage();

            await store.AddAsync(message, CancellationToken.None);

            db.Set<DeadLetterMessage>().Should().HaveCount(1);
        }

        [Fact]
        public async Task Should_return_only_pending_messages()
        {
            var db = CreateDb();
            var store = new EfDeadLetterStore(db);

            var now = DateTime.UtcNow;

            var valid = CreateValidMessage(failedAt: now.AddMinutes(-1));

            var future = CreateValidMessage();
            future.NextRetryAt = now.AddMinutes(10);

            var processed = CreateValidMessage();
            processed.Reprocessed = true;

            db.AddRange(valid, future, processed);
            await db.SaveChangesAsync();

            var result = await store.GetPendingAsync(10, CancellationToken.None);

            result.Should().ContainSingle(x => x.MessageId == valid.MessageId);
        }

        [Fact]
        public async Task Should_mark_message_as_reprocessed()
        {
            var db = CreateDb();
            var store = new EfDeadLetterStore(db);

            var message = CreateValidMessage();

            db.Add(message);
            await db.SaveChangesAsync();

            await store.MarkReprocessedAsync(message.MessageId, CancellationToken.None);

            var updated = await db.Set<DeadLetterMessage>().FirstAsync();

            updated.Reprocessed.Should().BeTrue();
        }

        [Fact]
        public async Task Should_ignore_mark_reprocessed_when_not_found()
        {
            var db = CreateDb();
            var store = new EfDeadLetterStore(db);

            await store.MarkReprocessedAsync(Guid.NewGuid(), CancellationToken.None);

            db.Set<DeadLetterMessage>().Should().BeEmpty();
        }

        [Fact]
        public async Task Should_update_retry_info()
        {
            var db = CreateDb();
            var store = new EfDeadLetterStore(db);

            var message = CreateValidMessage();

            db.Add(message);
            await db.SaveChangesAsync();

            var nextRetry = DateTime.UtcNow.AddMinutes(5);

            await store.UpdateRetryAsync(
                message.MessageId,
                reprocessCount: 3,
                nextRetryAt: nextRetry,
                CancellationToken.None);

            var updated = await db.Set<DeadLetterMessage>().FirstAsync();

            updated.ReprocessCount.Should().Be(3);
            updated.NextRetryAt.Should().BeCloseTo(nextRetry, TimeSpan.FromSeconds(1));
        }

        [Fact]
        public async Task Should_ignore_update_retry_when_not_found()
        {
            var db = CreateDb();
            var store = new EfDeadLetterStore(db);

            await store.UpdateRetryAsync(
                Guid.NewGuid(),
                2,
                DateTime.UtcNow,
                CancellationToken.None);

            db.Set<DeadLetterMessage>().Should().BeEmpty();
        }

        private static DeadLetterMessage CreateValidMessage(Guid? id = null, DateTime? failedAt = null)
        {
            return new DeadLetterMessage
            {
                MessageId = id ?? Guid.NewGuid(),
                MessageType = "test",
                PayloadJson = "{}",
                Error = "error",
                StackTrace = "stack",
                FailedAt = failedAt ?? DateTime.UtcNow,
                Reprocessed = false,
                ReprocessCount = 0,
                RetryCount = 0
            };
        }
    }
}