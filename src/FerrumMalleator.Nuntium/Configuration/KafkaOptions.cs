using Confluent.Kafka;
using System.Diagnostics.CodeAnalysis;

namespace FerrumMalleator.Nuntium.Configuration
{
    /// <summary>
    /// Configuration options for Kafka transport.
    /// </summary>
    /// <remarks>
    /// Provides settings for producer and consumer behavior,
    /// including connection, partitioning and advanced configurations.
    /// </remarks>    
    [ExcludeFromCodeCoverage]
    public sealed class KafkaOptions
    {
        /// <summary>
        /// Gets or sets the Kafka bootstrap servers.
        /// </summary>
        /// <remarks>
        /// This defines the initial Kafka brokers used to establish the connection.
        /// Example: "localhost:9092".
        /// </remarks>
        public string BootstrapServers { get; set; } = "localhost:9092";

        /// <summary>
        /// Gets or sets the default number of partitions for topics.
        /// </summary>
        /// <remarks>
        /// Used when provisioning topics automatically.
        /// </remarks>
        public int DefaultNumPartitions { get; set; } = 3;

        /// <summary>
        /// Gets or sets the default replication factor for topics.
        /// </summary>
        /// <remarks>
        /// Defines how many replicas each partition will have.
        /// </remarks>
        public short DefaultReplicationFactor { get; set; } = 1;

        /// <summary>
        /// Gets or sets the partition key resolver.
        /// </summary>
        /// <remarks>
        /// Internal configuration used to determine how messages are distributed across partitions.
        /// </remarks>
        internal Func<object, string>? PartitionKeyResolver { get; set; }

        /// <summary>
        /// Gets or sets the producer configuration action.
        /// </summary>
        /// <remarks>
        /// Allows customization of Kafka producer settings such as acknowledgments,
        /// batching and timeouts.
        /// </remarks>
        internal Action<ProducerConfig>? ProducerConfigAction { get; set; }

        /// <summary>
        /// Gets or sets the consumer configuration action.
        /// </summary>
        /// <remarks>
        /// Allows customization of Kafka consumer settings such as fetch size,
        /// offset reset strategy and commit behavior.
        /// </remarks>
        internal Action<ConsumerConfig>? ConsumerConfigAction { get; set; }

        /// <summary>
        /// Configures the Kafka producer.
        /// </summary>
        /// <param name="configure">Action to configure the producer settings.</param>
        /// <returns>The current <see cref="KafkaOptions"/> instance.</returns>
        /// <remarks>
        /// This allows fine-tuning of producer behavior such as acknowledgments,
        /// retries, batching and message timeouts.
        /// </remarks>
        /// <example>
        /// <code>
        /// options.ConfigureProducer(config =>
        /// {
        ///     config.Acks = Acks.All;
        ///     config.LingerMs = 5;
        /// });
        /// </code>
        /// </example>
        public KafkaOptions ConfigureProducer(Action<ProducerConfig> configure)
        {
            ProducerConfigAction += configure;
            return this;
        }

        /// <summary>
        /// Configures the Kafka consumer.
        /// </summary>
        /// <param name="configure">Action to configure the consumer settings.</param>
        /// <returns>The current <see cref="KafkaOptions"/> instance.</returns>
        /// <remarks>
        /// This allows customization of consumer behavior such as offset reset,
        /// fetch size and auto commit settings.
        /// </remarks>
        /// <example>
        /// <code>
        /// options.ConfigureConsumer(config =>
        /// {
        ///     config.AutoOffsetReset = AutoOffsetReset.Earliest;
        ///     config.EnableAutoCommit = true;
        /// });
        /// </code>
        /// </example>
        public KafkaOptions ConfigureConsumer(Action<ConsumerConfig> configure)
        {
            ConsumerConfigAction += configure;
            return this;
        }
    }
}
