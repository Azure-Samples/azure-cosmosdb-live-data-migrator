using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Azure.Identity;
using Microsoft.ApplicationInsights.Extensibility;
using Microsoft.Azure.Cosmos;
using Migration.Shared;
using Migration.Shared.DataContracts;

namespace Migration.Monitor.WebJob
{
    internal class Program
    {
        public const string SourceName = "MigrationMonitor";
        public const string MigrationClientUserAgentPrefix = "MigrationMonitor.MigrationMetadata";
        public const string SourceClientUserAgentPrefix = "MigrationMonitor.Source";
        public const string DestinationClientUserAgentPrefix = "MigrationMonitor.Destination";

        private const int SleepTime = 10000;
        private const int MaxConcurrentMonitoringJobs = 5;

        private static readonly Dictionary<string, CosmosClient> sourceClients =
            new Dictionary<string, CosmosClient>(StringComparer.OrdinalIgnoreCase);

        private static readonly Dictionary<string, CosmosClient> destinationClients =
            new Dictionary<string, CosmosClient>(StringComparer.OrdinalIgnoreCase);

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

                RunAsync().Wait();
            }
            catch (Exception unhandledException)
            {
                TelemetryHelper.Singleton.LogError(
                    "UNHANDLED EXCEPTION: {0}",
                    unhandledException);

                throw;
            }
        }

        private static async Task RunAsync()
        {
            await Task.Yield();
            SemaphoreSlim concurrencySemaphore = new SemaphoreSlim(MaxConcurrentMonitoringJobs);

            using (CosmosClient client =
               KeyVaultHelper.Singleton.CreateCosmosClientFromKeyVault(
                   EnvironmentConfig.Singleton.MigrationMetadataCosmosAccountName,
                   MigrationClientUserAgentPrefix,
                   useBulk: false,
                   retryOn429Forever: true))
            {
                Database db = await client
                    .CreateDatabaseIfNotExistsAsync(EnvironmentConfig.Singleton.MigrationMetadataDatabaseName)
                    .ConfigureAwait(false);

                Container container = await db
                    .CreateContainerIfNotExistsAsync(
                        new ContainerProperties(EnvironmentConfig.Singleton.MigrationMetadataContainerName, "/id"))
                    .ConfigureAwait(false);

                while (true)
                {
                    List<MigrationConfig> configDocs = new List<MigrationConfig>();
                    FeedIterator<MigrationConfig> iterator = container.GetItemQueryIterator<MigrationConfig>(
                        "select * from c where NOT c.completed");

                    while (iterator.HasMoreResults)
                    {
                        FeedResponse<MigrationConfig> response = await iterator.ReadNextAsync().ConfigureAwait(false);
                        configDocs.AddRange(response.Resource);
                    }

                    if (configDocs.Count == 0)
                    {
                        TelemetryHelper.Singleton.LogInfo(
                            "No Migration to monitor for process '{0}'",
                            Process.GetCurrentProcess().Id);
                    }
                    else
                    {
                        TelemetryHelper.Singleton.LogInfo(
                            "Starting to monitor migration by process '{0}'",
                            Process.GetCurrentProcess().Id);

                        Task[] tasks = new Task[configDocs.Count];
                        for (int i = 0; i < tasks.Length; i++)
                        {
                            await concurrencySemaphore.WaitAsync().ConfigureAwait(false);
                            await TrackMigrationProgressAsync(container, configDocs[i], concurrencySemaphore)
                                .ConfigureAwait(false);
                        }
                    }

                    await Task.Delay(SleepTime).ConfigureAwait(false);
                }
            }
        }

        private static async Task<long> GetDoucmentCountAsync(Container container)
        {
            if (container == null) { throw new ArgumentNullException(nameof(container)); }

            FeedIterator<long> iterator = container.GetItemQueryIterator<long>(
                "SELECT VALUE COUNT(1) FROM c");

            return (await iterator.ReadNextAsync().ConfigureAwait(false)).Resource.Single();
        }

        private static async Task TrackMigrationProgressAsync(
            Container migrationContainer,
            MigrationConfig migrationConfig,
            SemaphoreSlim concurrencySempahore)
        {
            if (migrationContainer == null) { throw new ArgumentNullException(nameof(migrationContainer)); }
            if (migrationConfig == null) { throw new ArgumentNullException(nameof(migrationConfig)); }
            if (concurrencySempahore == null) { throw new ArgumentNullException(nameof(concurrencySempahore)); }

            try
            {
                CosmosClient sourceClient = GetOrCreateSourceCosmosClient(migrationConfig.MonitoredAccount);
                CosmosClient destinationClient = GetOrCreateSourceCosmosClient(migrationConfig.DestAccount);
                Container sourceContainer = sourceClient.GetContainer(
                    migrationConfig.MonitoredDbName,
                    migrationConfig.MonitoredCollectionName);
                Container destinationContainer = destinationClient.GetContainer(
                    migrationConfig.DestDbName,
                    migrationConfig.DestCollectionName);

                while (true)
                {
                    MigrationConfig migrationConfigSnapshot = await migrationContainer
                        .ReadItemAsync<MigrationConfig>(migrationConfig.Id, new PartitionKey(migrationConfig.Id))
                        .ConfigureAwait(false);

                    DateTimeOffset now = DateTimeOffset.UtcNow;

                    long sourceCollectionCount = await GetDoucmentCountAsync(sourceContainer).ConfigureAwait(false);
                    long currentDestinationCollectionCount = await GetDoucmentCountAsync(destinationContainer)
                        .ConfigureAwait(false);
                    double currentPercentage = sourceCollectionCount == 0 ?
                        100 :
                        currentDestinationCollectionCount * 100.0 / sourceCollectionCount;
                    long insertedCount =
                        currentDestinationCollectionCount - migrationConfigSnapshot.MigratedDocumentCount;

                    long lastMigrationActivityRecorded = Math.Max(
                            migrationConfigSnapshot.StatisticsLastMigrationActivityRecordedEpochMs,
                            migrationConfigSnapshot.StartTimeEpochMs);
                    long nowEpochMs = now.ToUnixTimeMilliseconds();

                    double currentRate = insertedCount * 1000.0 / (nowEpochMs - lastMigrationActivityRecorded);
                    
                    long totalSeconds = 
                        (lastMigrationActivityRecorded - migrationConfigSnapshot.StartTimeEpochMs) / 1000;
                    double averageRate = totalSeconds > 0 ? currentDestinationCollectionCount / totalSeconds : 0;

                    long etaMs = averageRate > 0
                        ? (long)((sourceCollectionCount - currentDestinationCollectionCount) * 1000 / (averageRate * 3600))
                        : (long)TimeSpan.FromDays(100).TotalMilliseconds - migrationConfigSnapshot.StartTimeEpochMs;

                    migrationConfigSnapshot.ExpectedDurationLeft = etaMs;
                    migrationConfigSnapshot.AvgRate = averageRate;
                    migrationConfigSnapshot.CurrentRate = currentRate;
                    migrationConfigSnapshot.SourceCountSnapshot = sourceCollectionCount;
                    migrationConfigSnapshot.DestinationCountSnapshot = currentDestinationCollectionCount;
                    migrationConfigSnapshot.PercentageCompleted = currentPercentage;
                    migrationConfigSnapshot.StatisticsLastUpdatedEpochMs = nowEpochMs;
                    migrationConfigSnapshot.MigratedDocumentCount = currentDestinationCollectionCount;
                    if (insertedCount > 0)
                    {
                        migrationConfigSnapshot.StatisticsLastMigrationActivityRecordedEpochMs = nowEpochMs;
                    }

                    try
                    {
                        ItemResponse<MigrationConfig> response = await migrationContainer
                            .ReplaceItemAsync(
                                migrationConfigSnapshot,
                                migrationConfigSnapshot.Id,
                                new PartitionKey(migrationConfigSnapshot.Id),
                                new ItemRequestOptions
                                {
                                    IfMatchEtag = migrationConfigSnapshot.ETag
                                })
                            .ConfigureAwait(false);
                    }
                    catch (CosmosException error)
                    {
                        if (error.StatusCode == System.Net.HttpStatusCode.PreconditionFailed)
                        {
                            continue;
                        }

                        throw;
                    }

                    TelemetryHelper.Singleton.TrackStatistics(
                        sourceCollectionCount,
                        currentDestinationCollectionCount,
                        currentPercentage,
                        currentRate,
                        averageRate,
                        etaMs);

                    return;
                }
            }
            finally
            {
                concurrencySempahore.Release();
            }
        }

        private static CosmosClient GetOrCreateSourceCosmosClient(string accountName)
        {
            return GetOrCreateCosmosClient(
                sourceClients,
                SourceClientUserAgentPrefix,
                accountName);
        }

        private static CosmosClient GetOrCreateDestinationCosmosClient(string accountName)
        {
            return GetOrCreateCosmosClient(
                destinationClients,
                DestinationClientUserAgentPrefix,
                accountName);
        }

        private static CosmosClient GetOrCreateCosmosClient(
            Dictionary<string, CosmosClient> cache,
            string userAgentPrefix,
            string accountName)
        {
            if (cache == null) { throw new ArgumentNullException(nameof(cache)); }
            if (String.IsNullOrWhiteSpace(accountName)) { throw new ArgumentNullException(nameof(accountName)); }

            lock (cache)
            {
                if (cache.TryGetValue(accountName, out CosmosClient client))
                {
                    return client;
                }

                client = KeyVaultHelper.Singleton.CreateCosmosClientFromKeyVault(
                    accountName,
                    userAgentPrefix,
                    useBulk: false,
                    retryOn429Forever: true);
                cache.Add(accountName, client);

                return client;
            }
        }
    }
}