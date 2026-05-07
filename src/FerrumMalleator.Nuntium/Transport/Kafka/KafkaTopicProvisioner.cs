using Confluent.Kafka;
using Confluent.Kafka.Admin;
using FerrumMalleator.Nuntium.Builders;
using FerrumMalleator.Nuntium.Configuration;
using FerrumMalleator.Nuntium.Diagnostics;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using System.Diagnostics;

namespace FerrumMalleator.Nuntium.Transport.Kafka
{
    internal sealed class KafkaTopicProvisioner(IKafkaAdminClient admin, IOptions<KafkaOptions> options, MessageMetadataRegistry registry) : BackgroundService
    {
        private readonly KafkaOptions _options = options.Value;

        private readonly MessageMetadataRegistry _registry = registry;

        private readonly IKafkaAdminClient _admin = admin;

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            await ProvisionAsync(stoppingToken);
        }

        internal async Task ProvisionAsync(CancellationToken ct)
        {
            using var activity = NuntiumDiagnostics.ActivitySource.StartActivity("nuntium.kafka.provision", ActivityKind.Internal);

            var start = Stopwatch.GetTimestamp();

            activity?.SetTag("messaging.system", "kafka");

            activity?.SetTag("messaging.operation", "provision");

            activity?.SetTag("messaging.kafka.bootstrap_servers", _options.BootstrapServers);

            try
            {
                var topics = _registry.GetAll()
                        .Select(x => new TopicSpecification
                        {
                            Name = x.Topic,
                            NumPartitions = _options.DefaultNumPartitions,
                            ReplicationFactor = _options.DefaultReplicationFactor
                        })
                        .ToList();

                activity?.SetTag("messaging.kafka.topic_count", topics.Count);

                if (topics.Count == 0)
                {
                    activity?.AddEvent(new ActivityEvent("kafka.provision.no_topics"));

                    return;
                }

                foreach (var topic in topics)
                {
                    activity?.AddEvent(new ActivityEvent("kafka.topic.provisioning",
                            tags: new ActivityTagsCollection
                            {
                                { "messaging.destination.name", topic.Name }
                            }));
                }

                await _admin.CreateTopicsAsync(topics);

                activity?.SetStatus(ActivityStatusCode.Ok);

                NuntiumDiagnostics.KafkaTopicsCreated.Add(topics.Count);
            }
            catch (CreateTopicsException ex)
            {
                foreach (var result in ex.Results)
                {
                    if (result.Error.Code != ErrorCode.TopicAlreadyExists)
                    {
                        activity?.SetStatus(ActivityStatusCode.Error);

                        activity?.AddException(ex);

                        NuntiumDiagnostics.MessagesFailed.Add(1);

                        throw;
                    }

                    activity?.AddEvent(new ActivityEvent("kafka.topic.already_exists",
                            tags: new ActivityTagsCollection
                            {
                                { "messaging.destination.name", result.Topic }
                            }));
                }
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

                NuntiumDiagnostics.KafkaProvisioningDuration.Record(elapsed.TotalMilliseconds);
            }
        }
    }
}