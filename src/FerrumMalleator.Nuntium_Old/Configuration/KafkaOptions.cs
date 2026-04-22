using Confluent.Kafka;

namespace FerrumMalleator.Nuntium.Configuration
{
    public class KafkaOptions
    {
        public string BootstrapServers { get; set; } = default;
        public SecurityProtocol SecurityProtocol { get; set; }
        public string SaslUsername { get; set; }
        public string SaslPassword { get; set; }
    }
}
