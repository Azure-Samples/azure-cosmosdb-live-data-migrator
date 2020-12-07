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
        public const string SourceName = "MigrationExecutor";
        public const string MigrationClientUserAgentPrefix = "MigrationExecutor.MigrationMetadata";
        public const string SourceClientUserAgentPrefix = "MigrationExecutor.Source";
        public const string DestinationClientUserAgentPrefix = "MigrationExecutor.Destination";

        private const int SleepTime = 5000;

        private readonly Dictionary<string, ChangeFeedProcessorHost> changeFeedProcessorHosts =
            new Dictionary<string, ChangeFeedProcessorHost>(StringComparer.OrdinalIgnoreCase);

#pragma warning disable IDE0060 // Remove unused parameter

        private static void Main(string[] args)
#pragma warning restore IDE0060 // Remove unused parameter
        {
            try
            {
                EnvironmentConfig.Initialize();

                TelemetryConfiguration telemetryConfig = new TelemetryConfiguration(
                    EnvironmentConfig.Singleton.AppInsightsInstrumentationKey);
                TelemetryHelper.Initilize(telemetryConfig, SourceName, Environment.MachineName);
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
                    FeedResponse<MigrationConfig> response = await container
                        .GetItemQueryIterator<MigrationConfig>("select * from c where NOT c.completed")
                        .ReadNextAsync()
                        .ConfigureAwait(false);

                    List<MigrationConfig> configDocs = response.AsEnumerable<MigrationConfig>().ToList();

                    if (configDocs.Count == 0)
                    {
                        TelemetryHelper.Singleton.LogInfo("No job for process: {0}", Process.GetCurrentProcess().Id);
                        await Task.Delay(SleepTime);
                        continue;
                    }

                    foreach (MigrationConfig config in configDocs)
                    {
                        if (!config.Completed &&
                            !this.changeFeedProcessorHosts.ContainsKey(config.ProcessorName))
                        {
                            TelemetryHelper.Singleton.LogInfo(
                                "Starting new changefeed processor '{0}' for uncompleted migration",
                                config.ProcessorName);

                            ChangeFeedProcessorHost host = new ChangeFeedProcessorHost(config);
                            await host.StartAsync().ConfigureAwait(false);
                            this.changeFeedProcessorHosts.Add(
                                config.ProcessorName,
                                host);
                        }
                    }

                    foreach (string key in this.changeFeedProcessorHosts.Keys.ToArray())
                    {
                        if (!configDocs.Exists((c) => !c.Completed && c.ProcessorName == key))
                        {
                            TelemetryHelper.Singleton.LogInfo(
                                "Closing changefeed processor '{0} because migration has been completed",
                                key);

                            // Migration has been completed for this processor
                            await this.changeFeedProcessorHosts[key].CloseAsync().ConfigureAwait(false);
                            this.changeFeedProcessorHosts.Remove(key);
                        }
                    }

                    await Task.Delay(SleepTime);
                }
            }
        }
    }
}