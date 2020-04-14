using Microsoft.ApplicationInsights;
using Microsoft.ApplicationInsights.Extensibility;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace MigrationProgressApp
{
    class Program
    {
        public static TelemetryClient telemetryClient = new TelemetryClient();

        private static string migrationDetailsEndpoint = ConfigurationManager.AppSettings["cosmosdbaccount"];
        private static string migrationDetailsKey = ConfigurationManager.AppSettings["cosmosdbkey"];
        private static string migrationDetailsDB = ConfigurationManager.AppSettings["cosmosdbdb"];
        private static string migrationDetailsColl = ConfigurationManager.AppSettings["cosmosdbcollection"];
        private static string appInsightsInstrumentationKey = ConfigurationManager.AppSettings["appinsightsinstrumentationkey"];

        const int sleepTime = 15000;

        private static Uri migrationDetailsCollectionUri = UriFactory.CreateDocumentCollectionUri(migrationDetailsDB, migrationDetailsColl);
        private static DocumentClient client = new DocumentClient(new Uri(migrationDetailsEndpoint), migrationDetailsKey);

        private long startTime = 0;
        private long sourceCollectionCount = 0;
        private double currentPercentage = 0;
        private long prevDestinationCollectionCount = 0;
        private long currentDestinationCollectionCount = 0;
        private double totalInserted = 0;
        private string currentMigrationId = null;

        static void Main(string[] args)
        {
            TelemetryConfiguration.Active.InstrumentationKey = appInsightsInstrumentationKey;
            new Program().RunAsync().Wait();
        }

        public async Task RunAsync()
        {
            var db = await client.CreateDatabaseIfNotExistsAsync(new Database() { Id = migrationDetailsDB });
            await client.CreateDocumentCollectionIfNotExistsAsync(db.Resource.SelfLink, new DocumentCollection() { Id = migrationDetailsColl });

            while (true)
            {
                Int32 d = (Int32)DateTime.UtcNow.Subtract(new DateTime(1970, 1, 1)).Subtract(TimeSpan.FromHours(10)).TotalSeconds;
                var option = new FeedOptions { EnableCrossPartitionQuery = true };
                var configDocs = client.CreateDocumentQuery<MigrationConfig>(UriFactory.CreateDocumentCollectionUri(migrationDetailsDB, migrationDetailsColl),
                    string.Format("select * from c where NOT c.completed", d), option).AsEnumerable<MigrationConfig>().ToList();

                if (configDocs.Count == 0)
                {
                    Console.WriteLine("No Migration to monitor for process " + Process.GetCurrentProcess().Id);
                } else {
                    MigrationConfig config = configDocs.First();

                    if(config.Id != currentMigrationId)
                    {
                        startTime = config.StartTime;
                        sourceCollectionCount = 0;
                        currentPercentage = 0;
                        prevDestinationCollectionCount = 0;
                        currentDestinationCollectionCount = 0;
                        totalInserted = 0;
                        currentMigrationId = config.Id;
                    }

                    Console.WriteLine("Starting to monitor migration by process " + Process.GetCurrentProcess().Id);
                    await TrackMigrationProgressAsync(configDocs.First());
                }

                await Task.Delay(10000);
            }
        }

        private async Task TrackMigrationProgressAsync(MigrationConfig migrationConfig)
        {
            using (DocumentClient sourceClient = new DocumentClient(new Uri(migrationConfig.MonitoredUri),
                migrationConfig.MonitoredSecretKey))
            {
                sourceClient.ConnectionPolicy.RetryOptions = new RetryOptions { MaxRetryAttemptsOnThrottledRequests = 1000, MaxRetryWaitTimeInSeconds = 1000 };
                using (DocumentClient destinationClient = new DocumentClient(new Uri(migrationConfig.DestUri),
                    migrationConfig.DestSecretKey))
                {
                    destinationClient.ConnectionPolicy.RetryOptions = new RetryOptions { MaxRetryAttemptsOnThrottledRequests = 1000, MaxRetryWaitTimeInSeconds = 1000 };

                    RequestOptions options = new RequestOptions()
                    {
                        PopulateQuotaInfo = true,
                        PopulatePartitionKeyRangeStatistics = true
                    };

                    ResourceResponse<DocumentCollection> sourceCollection = await sourceClient.ReadDocumentCollectionAsync(
                                    UriFactory.CreateDocumentCollectionUri(migrationConfig.MonitoredDbName, migrationConfig.MonitoredCollectionName), options);

                    sourceCollectionCount = sourceCollection.Resource.PartitionKeyRangeStatistics
                        .Sum(pkr => pkr.DocumentCount);

                    ResourceResponse<DocumentCollection> destinationCollection = await destinationClient.ReadDocumentCollectionAsync(
                        UriFactory.CreateDocumentCollectionUri(migrationConfig.DestDbName, migrationConfig.DestCollectionName), options);

                    currentDestinationCollectionCount = destinationCollection.Resource.PartitionKeyRangeStatistics
                        .Sum(pkr => pkr.DocumentCount);

                    currentPercentage = sourceCollectionCount == 0 ? 100 : currentDestinationCollectionCount * 100.0 / sourceCollectionCount;

                    double currentRate = (currentDestinationCollectionCount - prevDestinationCollectionCount) * 1000.0 / sleepTime;
                    totalInserted += prevDestinationCollectionCount == 0 ? 0 : currentDestinationCollectionCount - prevDestinationCollectionCount;

                    DateTime currentTime = DateTime.UtcNow;
                    long totalSeconds = ((long)DateTime.UtcNow.Subtract(new DateTime(1970, 1, 1)).TotalMilliseconds - startTime) / 1000;
                    double averageRate = totalInserted * 1.0 / totalSeconds;
                    double eta = averageRate == 0 ? 0 : (sourceCollectionCount - currentDestinationCollectionCount) * 1.0 / (averageRate * 3600);

                    trackMetrics(telemetryClient, sourceCollectionCount, currentDestinationCollectionCount, currentRate, averageRate, eta);

                    prevDestinationCollectionCount = currentDestinationCollectionCount;

                    await PersistMetrics(migrationConfig, currentRate, averageRate, eta);
                }
            }
        }

        private async Task PersistMetrics(MigrationConfig migrationConfig, double currentRate, double averageRate, double eta)
        {
            migrationConfig.Eta = eta;
            migrationConfig.AvergageInsertRate = averageRate;
            migrationConfig.CurrentInsertRate = currentRate;
            migrationConfig.SourceCollectionCount = sourceCollectionCount;
            migrationConfig.DestinationCollectionCount = currentDestinationCollectionCount;
            migrationConfig.PercentageCompleted = currentPercentage;

            await client.UpsertDocumentAsync(migrationDetailsCollectionUri, migrationConfig,
                new RequestOptions()
                {
                    AccessCondition = new AccessCondition()
                    {
                        Condition = migrationConfig.Etag,
                        Type = AccessConditionType.IfNoneMatch
                    }
                });
        }

        private void trackMetrics(TelemetryClient telemetryClient, long sourceCollectionCount, long currentDestinationCollectionCount, double currentRate, double averageRate, double eta)
        {
            telemetryClient.TrackMetric("SourceCollectionCount", sourceCollectionCount);
            telemetryClient.TrackMetric("DestinationCollectionCount", currentDestinationCollectionCount);
            telemetryClient.TrackMetric("CurrentPercentage", currentPercentage);
            telemetryClient.TrackMetric("CurrentInsertRate", currentRate);
            telemetryClient.TrackMetric("AverageInsertRate", averageRate);
            telemetryClient.TrackMetric("ETA", eta);

            Console.WriteLine("CurrentPercentage = " + currentPercentage, currentPercentage);
            Console.WriteLine("ETA = " + eta);
            Console.WriteLine("Current rate = " + currentRate);
            Console.WriteLine("Average rate = " + averageRate);
            Console.WriteLine("Source count = " + sourceCollectionCount);
            Console.WriteLine("Destination count = " + currentDestinationCollectionCount);

            Console.WriteLine("Finished publishing metrics .. ");
            telemetryClient.Flush();
        }
    }
}
