using Microsoft.Extensions.Logging;

namespace FerrumMalleator.Nuntium.Tests.Fake
{
    internal class FakeLogger : ILogger
    {
        public List<string> Logs = new();

        public IDisposable BeginScope<TState>(TState state) => null!;
        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId,
            TState state, Exception exception,
            Func<TState, Exception, string> formatter)
        {
            Logs.Add(formatter(state, exception));
        }
    }
}
