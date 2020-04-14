using Microsoft.ApplicationInsights;
using Microsoft.ApplicationInsights.Extensibility;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Microsoft.Azure.Documents.Linq;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace MigrationConsoleApp
{
    class Program
    {
        public static TelemetryClient telemetryClient = new TelemetryClient();

        private static string endpoint = ConfigurationManager.AppSettings["cosmosdbaccount"];
        private static string masterkey = ConfigurationManager.AppSettings["cosmosdbkey"];
        private static string jobdb = ConfigurationManager.AppSettings["cosmosdbdb"];
        private static string jobColl = ConfigurationManager.AppSettings["cosmosdbcollection"];
        private static string appInsightsInstrumentationKey = ConfigurationManager.AppSettings["appinsightsinstrumentationkey"];
        private static DocumentClient client = new DocumentClient(new Uri(endpoint), masterkey);
        private string currentMigrationId = null;
        private ChangeFeedProcessorHost changeFeedProcessorHost = null;
        static void Main(string[] args)
        {
            TelemetryConfiguration.Active.InstrumentationKey = appInsightsInstrumentationKey;
            new Program().RunAsync().Wait();
        }

        public async Task RunAsync()
        {
            var db = await client.CreateDatabaseIfNotExistsAsync(new Database() { Id = jobdb });
            await client.CreateDocumentCollectionIfNotExistsAsync(db.Resource.SelfLink, new DocumentCollection() { Id = jobColl });

            while (true) {
                // Check if a migration doc got inserted in the last hour
                if (currentMigrationId == null) {
                    var option = new FeedOptions { EnableCrossPartitionQuery = true };
                    var configDocs = client.CreateDocumentQuery<MigrationConfig>(UriFactory.CreateDocumentCollectionUri(jobdb, jobColl),
                        string.Format("select * from c where NOT c.completed"), option).AsEnumerable<MigrationConfig>().ToList();

                    if (configDocs.Count == 0) {
                        Console.WriteLine("No job for process: " + Process.GetCurrentProcess().Id);
                        await Task.Delay(5000);
                        continue;
                    }

                    var config = configDocs.First();
                    currentMigrationId = config.Id;
                    changeFeedProcessorHost = new ChangeFeedProcessorHost(config);
                    await changeFeedProcessorHost.StartAsync();
                } else
                {
                    var option = new FeedOptions { EnableCrossPartitionQuery = true };
                    var configDocs = client.CreateDocumentQuery<MigrationConfig>(UriFactory.CreateDocumentCollectionUri(jobdb, jobColl),
                        string.Format("select * from c where c.id = \"{0}\"", currentMigrationId), option).AsEnumerable<MigrationConfig>().ToList();
                    if (configDocs.Count == 0 || configDocs.First().Completed)
                    {
                        Console.WriteLine("Current Migration is completed or deleted, closing migration " + Process.GetCurrentProcess().Id);
                        this.currentMigrationId = null;
                        await changeFeedProcessorHost.CloseAsync();
                        this.changeFeedProcessorHost = null;
                        continue;
                    }

                    await Task.Delay(5000);
                }
            }
        }
    }
}

