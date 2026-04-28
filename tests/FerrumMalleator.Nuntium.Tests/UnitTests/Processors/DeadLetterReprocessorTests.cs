using FerrumMalleator.Nuntium.Abstractions.Persistence;
using FerrumMalleator.Nuntium.Abstractions.Publishers;
using FerrumMalleator.Nuntium.DeadLetter;
using FerrumMalleator.Nuntium.Messaging.Models;
using FerrumMalleator.Nuntium.Persistence.InMemory;
using FerrumMalleator.Nuntium.Tests.Fake;
using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;

namespace FerrumMalleator.Nuntium.Tests.UnitTests.Processors
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
    }
}
