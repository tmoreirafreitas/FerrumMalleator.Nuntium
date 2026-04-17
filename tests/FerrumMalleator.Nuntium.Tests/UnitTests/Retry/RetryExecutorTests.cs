using FerrumMalleator.Nuntium.Configuration;
using FerrumMalleator.Nuntium.Retry;
using FluentAssertions;

namespace FerrumMalleator.Nuntium.Tests.UnitTests.Retry;

public class RetryExecutorTests
{
    [Fact]
    public async Task Should_retry_until_success()
    {
        var attempts = 0;

        var options = new RetryPolicyOptions
        {
            MaxAttempts = 3,
            Delays = [TimeSpan.FromMilliseconds(10)]
        };

        var executor = new RetryExecutor(options);

        await executor.ExecuteAsync(async () =>
        {
            attempts++;

            if (attempts < 2)
                throw new Exception();

            await Task.CompletedTask;
        },
        (ex, retry) =>
        {
            return Task.CompletedTask;
        });

        attempts.Should().Be(2);
    }

    [Fact]
    public async Task Should_call_failure_after_max_attempts()
    {
        var attempts = 0;
        var failureCalled = false;

        var options = new RetryPolicyOptions
        {
            MaxAttempts = 2,
            Delays = [TimeSpan.FromMilliseconds(10)]
        };

        var executor = new RetryExecutor(options);

        Func<Task> act = () => executor.ExecuteAsync(async () =>
        {
            attempts++;
            throw new Exception("fail");
        },
        (ex, retry) =>
        {
            failureCalled = true;
            return Task.CompletedTask;
        });

        await act.Should().ThrowAsync<Exception>();

        failureCalled.Should().BeTrue();
        attempts.Should().Be(2);
    }
}