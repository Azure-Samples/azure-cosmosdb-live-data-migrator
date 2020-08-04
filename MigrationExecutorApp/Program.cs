using System;
using System.Configuration;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.ApplicationInsights;
using Microsoft.ApplicationInsights.Extensibility;
using Microsoft.Azure.Cosmos;

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

        private static CosmosClient client = new CosmosClient(endpoint,masterkey);
        private string currentMigrationId = null;
        private ChangeFeedProcessorHost changeFeedProcessorHost = null;
        static void Main(string[] args)
        {
            TelemetryConfiguration.Active.InstrumentationKey = appInsightsInstrumentationKey;
            new Program().RunAsync().Wait();
        }

        public async Task RunAsync()
        {

            Database db = await client.CreateDatabaseIfNotExistsAsync(jobdb);            

            Container container = await db.CreateContainerIfNotExistsAsync(new ContainerProperties(jobColl, "/_partitionKey"));

            while (true) {
                // Check if a migration doc got inserted in the last hour
                if (currentMigrationId == null) {

                    var configDocs = container.GetItemQueryIterator<MigrationConfig>("select * from c where NOT c.completed").ReadNextAsync().Result.AsEnumerable<MigrationConfig>().ToList();

                    if (configDocs.Count == 0)
                    {
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
                    var configDocs = container.GetItemQueryIterator<MigrationConfig>(string.Format("select * from c where c.id = \"{0}\"", currentMigrationId)).ReadNextAsync().Result.AsEnumerable<MigrationConfig>().ToList();

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

