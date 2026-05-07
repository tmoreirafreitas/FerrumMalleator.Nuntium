using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Diagnostics.Metrics;

namespace FerrumMalleator.Nuntium.Diagnostics
{
    internal static class NuntiumDiagnostics
    {
        public static readonly ActivitySource ActivitySource = new("FerrumMalleator.Nuntium");

        public static readonly Meter Meter = new("FerrumMalleator.Nuntium");

        // =========================================================
        // Counters
        // =========================================================

        public static readonly Counter<long> MessagesConsumed = Meter.CreateCounter<long>("messages.consumed");

        public static readonly Counter<long> MessagesDispatched = Meter.CreateCounter<long>("messages.dispatched");

        public static readonly Counter<long> MessagesPublished = Meter.CreateCounter<long>("messages.published");

        public static readonly Counter<long> MessagesTransported = Meter.CreateCounter<long>("messages.transported");

        public static readonly Counter<long> MessagesFailed = Meter.CreateCounter<long>("messages.failed");

        public static readonly Counter<long> MessagesDeadlettered = Meter.CreateCounter<long>("messages.deadlettered");

        public static readonly Counter<long> MessagesReprocessed = Meter.CreateCounter<long>("messages.reprocessed");

        public static readonly Counter<long> MessagesReprocessFailed = Meter.CreateCounter<long>("messages.reprocess.failed");

        public static readonly Counter<long> OutboxMessagesStored = Meter.CreateCounter<long>("outbox.messages.stored");

        public static readonly Counter<long> OutboxMessagesProcessed = Meter.CreateCounter<long>("outbox.messages.processed");

        public static readonly Counter<long> OutboxFailures = Meter.CreateCounter<long>("outbox.failures");

        public static readonly Counter<long> SagaExecutions = Meter.CreateCounter<long>("saga.executions");

        public static readonly Counter<long> SagaFailures = Meter.CreateCounter<long>("saga.failures");

        public static readonly Counter<long> SagaStateLoads = Meter.CreateCounter<long>("saga.state.loads");

        public static readonly Counter<long> SagaStateSaves = Meter.CreateCounter<long>("saga.state.saves");

        public static readonly Counter<long> KafkaTopicsCreated = Meter.CreateCounter<long>("kafka.topics.created");

        public static readonly Counter<long> KafkaMessagesConsumed = Meter.CreateCounter<long>("kafka.messages.consumed");

        public static readonly Counter<long> KafkaMessagesPublished = Meter.CreateCounter<long>("kafka.messages.published");

        public static readonly Counter<long> KafkaPublishFailures = Meter.CreateCounter<long>("kafka.publish.failures");

        public static readonly Counter<long> KafkaCommitFailures = Meter.CreateCounter<long>("kafka.commit.failures");

        public static readonly Counter<long> KafkaConsumeFailures = Meter.CreateCounter<long>("kafka.consume.failures");

        // =========================================================
        // Histograms
        // =========================================================

        public static readonly Histogram<double> ConsumerDuration = Meter.CreateHistogram<double>("consumer.duration.ms");

        public static readonly Histogram<double> DispatchDuration = Meter.CreateHistogram<double>("dispatch.duration.ms");

        public static readonly Histogram<double> PublishDuration = Meter.CreateHistogram<double>("publish.duration.ms");

        public static readonly Histogram<double> TransportDuration = Meter.CreateHistogram<double>("transport.duration.ms");

        public static readonly Histogram<double> SagaDuration = Meter.CreateHistogram<double>("saga.duration.ms");

        public static readonly Histogram<double> DeadLetterReprocessDuration = Meter.CreateHistogram<double>("deadletter.reprocess.duration.ms");

        public static readonly Histogram<double> OutboxStoreDuration = Meter.CreateHistogram<double>("outbox.store.duration.ms");

        public static readonly Histogram<double> OutboxProcessDuration = Meter.CreateHistogram<double>("outbox.process.duration.ms");

        public static readonly Histogram<double> SagaHandlerDuration = Meter.CreateHistogram<double>("saga.handler.duration.ms");

        public static readonly Histogram<double> KafkaConsumeDuration = Meter.CreateHistogram<double>("kafka.consume.duration.ms");

        public static readonly Histogram<double> KafkaPublishDuration = Meter.CreateHistogram<double>("kafka.publish.duration.ms");

        public static readonly Histogram<double> KafkaTopicCreationDuration = Meter.CreateHistogram<double>("kafka.topic.creation.duration.ms");

        public static readonly Histogram<double> KafkaProvisioningDuration = Meter.CreateHistogram<double>("kafka.provisioning.duration.ms");
    }
}