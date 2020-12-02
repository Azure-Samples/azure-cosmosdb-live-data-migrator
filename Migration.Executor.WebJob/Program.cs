using Azure.Identity;
using Azure.Security.KeyVault.Secrets;
using Microsoft.ApplicationInsights.Extensibility;
using Microsoft.Azure.Cosmos;
using Migration.Shared;
using Migration.Shared.DataContracts;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Threading.Tasks;

namespace Migration.Executor.WebJob
{
    class Program
    {
        public const string MigrationClientUserAgentPrefix = "MigrationExecutor.MigrationMetadata";
        public const string LeaseClientUserAgentPrefix = "MigrationExecutor.MigrationLeases";
        public const string SourceClientUserAgentPrefix = "MigrationExecutor.Source";
        public const string DestinationClientUserAgentPrefix = "MigrationExecutor.Destination";

        const int SleepTime = 5000;

        private static readonly string keyVaultUri = ConfigurationManager.AppSettings["keyvaulturi"];
        private static readonly string migrationMetadataAccount = ConfigurationManager.AppSettings["cosmosdbaccount"];
        private static readonly string jobdb = ConfigurationManager.AppSettings["cosmosdbdb"];
        private static readonly string jobColl = ConfigurationManager.AppSettings["cosmosdbcollection"];
        private static readonly string appInsightsInstrumentationKey =
            ConfigurationManager.AppSettings["appinsightsinstrumentationkey"];

        private string currentMigrationId = null;
        private ChangeFeedProcessorHost changeFeedProcessorHost = null;

#pragma warning disable IDE0060 // Remove unused parameter
        static void Main(string[] args)
#pragma warning restore IDE0060 // Remove unused parameter
        {
            TelemetryConfiguration telemetryConfig = new TelemetryConfiguration(appInsightsInstrumentationKey);
            TelemetryHelper.Initilize(telemetryConfig);

            KeyVaultHelper.Initialize(new Uri(keyVaultUri), new DefaultAzureCredential());

            new Program().RunAsync().Wait();
        }

        public async Task RunAsync()
        {
            using (CosmosClient client =
                KeyVaultHelper.Singleton.CreateCosmosClientFromKeyVault(
                    migrationMetadataAccount,
                    MigrationClientUserAgentPrefix,
                    useBulk: false,
                    retryOn429Forever: true))
            {
                Database db = await client.CreateDatabaseIfNotExistsAsync(jobdb);

                Container container = await db.CreateContainerIfNotExistsAsync(new ContainerProperties(jobColl, "/id"));

                while (true)
                {
                    // Check if a migration doc got inserted in the last hour
                    if (this.currentMigrationId == null)
                    {
                        FeedResponse<MigrationConfig> response = await container
                            .GetItemQueryIterator<MigrationConfig>("select * from c where NOT c.completed")
                            .ReadNextAsync()
                            .ConfigureAwait(false);

                        List<MigrationConfig> configDocs = response.AsEnumerable<MigrationConfig>().ToList();

                        if (configDocs.Count == 0)
                        {
                            TelemetryHelper.Singleton.LogInfo("No job for process: {0}", Process.GetCurrentProcess().Id);
                            await Task.Delay(5000);
                            continue;
                        }

                        MigrationConfig config = configDocs.First();
                        this.currentMigrationId = config.Id;
                        this.changeFeedProcessorHost = new ChangeFeedProcessorHost(config);
                        await this.changeFeedProcessorHost.StartAsync();
                    }
                    else
                    {
                        MigrationConfig config;
                        try
                        {
                            config = await container
                                .ReadItemAsync<MigrationConfig>(
                                    this.currentMigrationId,
                                    new PartitionKey(this.currentMigrationId))
                                .ConfigureAwait(false);
                        }
                        catch (CosmosException error)
                        {
                            if (error.StatusCode != HttpStatusCode.NotFound)
                            {
                                throw;
                            }
                            else
                            {
                                TelemetryHelper.Singleton.LogInfo("Current Migration is deleted, closing migration { 0}", Process.GetCurrentProcess().Id);
                                this.currentMigrationId = null;
                                await this.changeFeedProcessorHost.CloseAsync();
                                this.changeFeedProcessorHost = null;

                                continue;
                            }
                        }

                        if (config.Completed)
                        {
                            TelemetryHelper.Singleton.LogInfo("Current Migration is completed, closing migration { 0}", Process.GetCurrentProcess().Id);
                            this.currentMigrationId = null;
                            await this.changeFeedProcessorHost.CloseAsync();
                            this.changeFeedProcessorHost = null;

                            continue;
                        }

                        await Task.Delay(SleepTime);
                    }
                }
            }
        }
    }
}
