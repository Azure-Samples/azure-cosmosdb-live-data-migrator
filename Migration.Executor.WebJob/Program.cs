using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Azure.Identity;
using Microsoft.ApplicationInsights.Extensibility;
using Microsoft.Azure.Cosmos;
using Migration.Shared;
using Migration.Shared.DataContracts;

namespace Migration.Executor.WebJob
{
    internal class Program
    {
        public const string MigrationClientUserAgentPrefix = "MigrationExecutor.MigrationMetadata";
        public const string SourceClientUserAgentPrefix = "MigrationExecutor.Source";
        public const string DestinationClientUserAgentPrefix = "MigrationExecutor.Destination";

        private const int SleepTime = 5000;

        private string currentMigrationId = null;
        private ChangeFeedProcessorHost changeFeedProcessorHost = null;

#pragma warning disable IDE0060 // Remove unused parameter

        private static void Main(string[] args)
#pragma warning restore IDE0060 // Remove unused parameter
        {
            try
            {
                EnvironmentConfig.Initialize();

                TelemetryConfiguration telemetryConfig = new TelemetryConfiguration(
                    EnvironmentConfig.Singleton.AppInsightsInstrumentationKey);
                TelemetryHelper.Initilize(telemetryConfig);
            }
            catch (Exception error)
            {
                Console.WriteLine(
                    "UNHANDLED EXCEPTION during initialization before TelemetryClient oculd be created: {0}",
                    error);

                throw;
            }

            try
            {
                KeyVaultHelper.Initialize(
                    new Uri(EnvironmentConfig.Singleton.KeyVaultUri),
                    new DefaultAzureCredential());

                new Program().RunAsync().Wait();
            }
            catch (Exception unhandledException)
            {
                TelemetryHelper.Singleton.LogError(
                    "UNHANDLED EXCEPTION: {0}",
                    unhandledException);

                throw;
            }
        }

        public async Task RunAsync()
        {
            await Task.Yield();

            using (CosmosClient client =
                KeyVaultHelper.Singleton.CreateCosmosClientFromKeyVault(
                    EnvironmentConfig.Singleton.MigrationMetadataCosmosAccountName,
                    MigrationClientUserAgentPrefix,
                    useBulk: false,
                    retryOn429Forever: true))
            {
                Database db = await client.CreateDatabaseIfNotExistsAsync(
                    EnvironmentConfig.Singleton.MigrationMetadataDatabaseName);

                Container container = await db.CreateContainerIfNotExistsAsync(
                    new ContainerProperties(EnvironmentConfig.Singleton.MigrationMetadataContainerName, "/id"));

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