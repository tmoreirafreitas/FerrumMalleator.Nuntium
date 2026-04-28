using FerrumMalleator.Nuntium.Abstractions.Publishers;

namespace FerrumMalleator.Nuntium.Tests.Fake
{
    internal class FakeTransport : IMessageTransport
    {
        public bool Sent;
        public bool Throw;

        public Task SendAsync(string topic, string message, CancellationToken ct)
        {
            if (Throw)
                throw new Exception("fail");

            Sent = true;
            return Task.CompletedTask;
        }
    }
}
