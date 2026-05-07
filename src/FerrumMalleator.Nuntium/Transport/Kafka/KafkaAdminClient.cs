using Confluent.Kafka;
using Confluent.Kafka.Admin;
using FerrumMalleator.Nuntium.Configuration;
using FerrumMalleator.Nuntium.Diagnostics;
using Microsoft.Extensions.Options;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;

namespace FerrumMalleator.Nuntium.Transport.Kafka
{
    [ExcludeFromCodeCoverage]
    internal sealed class KafkaAdminClient(IOptions<KafkaOptions> options) : IKafkaAdminClient
    {
        private readonly KafkaOptions _options = options.Value;

        public async Task CreateTopicsAsync(IEnumerable<TopicSpecification> topics)
        {
            using var activity = NuntiumDiagnostics.ActivitySource.StartActivity("nuntium.kafka.topics.create", ActivityKind.Client);

            var start = Stopwatch.GetTimestamp();

            activity?.SetTag("messaging.system", "kafka");

            activity?.SetTag("messaging.operation", "topic.create");

            activity?.SetTag("messaging.kafka.bootstrap_servers", _options.BootstrapServers);

            var topicList = topics.ToList();

            activity?.SetTag("messaging.kafka.topic_count", topicList.Count);

            try
            {
                using var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = _options.BootstrapServers }).Build();

                await admin.CreateTopicsAsync(topicList);

                foreach (var topic in topicList)
                {
                    activity?.AddEvent(new ActivityEvent("kafka.topic.created", tags: new ActivityTagsCollection
                        {
                            { "messaging.destination.name", topic.Name }
                        }));
                }

                activity?.SetStatus(ActivityStatusCode.Ok);

                NuntiumDiagnostics.KafkaTopicsCreated.Add(topicList.Count);
            }
            catch (Exception ex)
            {
                activity?.SetStatus(ActivityStatusCode.Error);

                activity?.AddException(ex);

                NuntiumDiagnostics.MessagesFailed.Add(1);

                throw;
            }
            finally
            {
                var elapsed = Stopwatch.GetElapsedTime(start);

                NuntiumDiagnostics.KafkaTopicCreationDuration.Record(elapsed.TotalMilliseconds);
            }
        }
    }
}