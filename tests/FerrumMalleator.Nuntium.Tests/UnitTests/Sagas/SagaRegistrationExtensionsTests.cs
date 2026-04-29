using FerrumMalleator.Nuntium.Abstractions;
using FerrumMalleator.Nuntium.Builders;
using FerrumMalleator.Nuntium.Sagas.Handlers;
using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using System.Reflection;

namespace FerrumMalleator.Nuntium.Tests.UnitTests.Sagas
{
    public class SagaRegistrationExtensionsTests
    {
        private record TestMessage(Guid Id);

        private class TestState : ISagaState
        {
            public Guid CorrelationId { get; set; }
        }

        private class TestSaga : ISagaHandler<TestMessage, TestState>
        {
            public Task HandleAsync(TestMessage message, TestState state, CancellationToken ct)
                => Task.CompletedTask;
        }

        private class NotASaga { }

        [Fact]
        public void Should_register_saga_handler_from_assembly()
        {
            // Arrange
            var services = new ServiceCollection();

            // Act
            services.AddSagaHandlers(typeof(TestSaga).Assembly);

            var provider = services.BuildServiceProvider();

            // Assert - handler registrado no DI
            var handler = provider.GetService<ISagaHandler<TestMessage, TestState>>();
            handler.Should().NotBeNull();

            // Assert - registry populado
            var registry = provider.GetRequiredService<SagaHandlerRegistry>();
            registry.Contains(typeof(TestMessage)).Should().BeTrue();
        }

        [Fact]
        public void Should_register_using_marker_type()
        {
            // Arrange
            var services = new ServiceCollection();

            // Act
            services.AddSagaHandlersFromAssemblyContaining<TestSaga>();

            var provider = services.BuildServiceProvider();

            // Assert
            var handler = provider.GetService<ISagaHandler<TestMessage, TestState>>();
            handler.Should().NotBeNull();
        }

        [Fact]
        public void Should_ignore_non_saga_types()
        {
            // Arrange
            var services = new ServiceCollection();

            // Act
            services.AddSagaHandlers(typeof(NotASaga).Assembly);

            var provider = services.BuildServiceProvider();

            var registry = provider.GetRequiredService<SagaHandlerRegistry>();

            // Assert
            registry.Contains(typeof(NotASaga)).Should().BeFalse();
        }

        [Fact]
        public void Should_register_multiple_sagas_from_assembly()
        {
            // Arrange
            var services = new ServiceCollection();

            // Act
            services.AddSagaHandlers(Assembly.GetExecutingAssembly());

            var provider = services.BuildServiceProvider();

            var registry = provider.GetRequiredService<SagaHandlerRegistry>();

            // Assert
            registry.Contains(typeof(TestMessage)).Should().BeTrue();
        }
    }
}