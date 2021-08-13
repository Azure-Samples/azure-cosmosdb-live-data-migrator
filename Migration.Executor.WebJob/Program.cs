using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Azure;
using Azure.Identity;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
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

        private const int SleepTimeInMs = 5000;

        private readonly ConcurrentDictionary<string, ChangeFeedProcessorHost> changeFeedProcessorHosts =
            new ConcurrentDictionary<string, ChangeFeedProcessorHost>(StringComparer.OrdinalIgnoreCase);

        private readonly SemaphoreSlim retryConcurrencySemaphore = new SemaphoreSlim(5);

        private static readonly ConcurrentDictionary<string, BlobContainerClient> deadletterClients =
            new ConcurrentDictionary<string, BlobContainerClient>(StringComparer.OrdinalIgnoreCase);

#pragma warning disable IDE0060 // Remove unused parameter

        private static void Main(string[] args)
#pragma warning restore IDE0060 // Remove unused parameter
        {
            try
            {
                EnvironmentConfig.Initialize();

                TelemetryConfiguration telemetryConfig = new TelemetryConfiguration(
                    EnvironmentConfig.Singleton.AppInsightsInstrumentationKey);
                TelemetryHelper.Initilize(telemetryConfig, SourceName);
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
                KeyVaultHelper.Initialize(new DefaultAzureCredential());

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

        private async Task RetryPoisonMessages(MigrationConfig config)
        {
            if (config == null) { throw new ArgumentNullException(nameof(config)); }

            await this.retryConcurrencySemaphore.WaitAsync().ConfigureAwait(false);

            try
            {
                if (!deadletterClients.TryGetValue(
                           config.ProcessorName,
                           out BlobContainerClient deadLetterClient))
                {
                    deadLetterClient = KeyVaultHelper.Singleton.GetBlobContainerClientFromKeyVault(
                        EnvironmentConfig.Singleton.DeadLetterAccountName,
                        config.Id?.ToLowerInvariant().Replace("-", String.Empty));

                    deadletterClients.TryAdd(config.ProcessorName, deadLetterClient);
                }

                AsyncPageable<BlobItem> blobsPagable = deadLetterClient.GetBlobsAsync(BlobTraits.All, BlobStates.None);
                await foreach (BlobItem blob in blobsPagable.ConfigureAwait(false))
                {
                    if (blob.Metadata.TryGetValue(
                        EnvironmentConfig.DeadLetterMetaDataSuccessfulRetryStatusKey,
                        out string successfulRetryStatusRaw) &&
                        long.TryParse(successfulRetryStatusRaw, out long successfulRetryStatus) &&
                        successfulRetryStatus > 0)
                    {
                        // all poison messages in this blob have been successfully retried
                        // safe to ignore
                        continue;
                    }

                    BlobClient blobClient = deadLetterClient.GetBlobClient(blob.Name);
                    MemoryStream downloadStream = new MemoryStream();
                    await blobClient.DownloadToAsync(downloadStream).ConfigureAwait(false);
                    string blobContent = Encoding.UTF8.GetString(downloadStream.ToArray());

                    int failedDocCount = Regex.Matches(blobContent, EnvironmentConfig.FailedDocSeperator).Count + 1;
                    if (!blob.Metadata.TryGetValue(
                        EnvironmentConfig.DeadLetterMetaSuccessfulRetryCountKey,
                        out string successfulRetryCountRaw) ||
                        !int.TryParse(successfulRetryCountRaw, out int successfulRetryCount))
                    {
                        successfulRetryCount = 0;
                    }

                    if (successfulRetryCount >= failedDocCount)
                    {
                        blob.Metadata[EnvironmentConfig.DeadLetterMetaDataSuccessfulRetryStatusKey] = "1";
                        _ = await blobClient.SetMetadataAsync(
                            blob.Metadata,
                            new BlobRequestConditions
                            {
                                IfMatch = blob.Properties.ETag
                            }).ConfigureAwait(false);

                        TelemetryHelper.Singleton.LogInfo(
                            "Updated metadata after retries for poison message blob '{0}' - " +
                            "SuccessfulRetryStatus: {1}, FailedDocCount: {2}, SuccesfulRetryCount: {3}",
                            blob.Name,
                            blob.Metadata[EnvironmentConfig.DeadLetterMetaDataSuccessfulRetryStatusKey],
                            failedDocCount,
                            blob.Metadata[EnvironmentConfig.DeadLetterMetaSuccessfulRetryCountKey]);

                        continue;
                    }

                    string[] failureColumns = blobContent.Split(EnvironmentConfig.FailureColumnSeperator);
                    if (failureColumns.Length != 3)
                    {
                        continue;
                    }

                    string[] failedDocs = failureColumns[2].Split(EnvironmentConfig.FailedDocSeperator);
                    List<DocumentIdentifier> failedDocIdentities = new List<DocumentIdentifier>();
                    foreach (string failedDocIdentifier in failedDocs)
                    {
                        failedDocIdentities.Add(DocumentIdentifier.FromString(failedDocIdentifier));
                    }

                    int successfullyMigratedCount = await this.changeFeedProcessorHosts[config.ProcessorName]
                        .RetryDocumentMigrations(failedDocIdentities)
                        .ConfigureAwait(false);

                    blob.Metadata[EnvironmentConfig.DeadLetterMetaSuccessfulRetryCountKey] =
                        successfullyMigratedCount.ToString(CultureInfo.InvariantCulture);

                    if (successfulRetryCount >= failedDocCount)
                    {
                        blob.Metadata[EnvironmentConfig.DeadLetterMetaDataSuccessfulRetryStatusKey] = "1";
                    }

                    _ = await blobClient.SetMetadataAsync(
                            blob.Metadata,
                            new BlobRequestConditions
                            {
                                IfMatch = blob.Properties.ETag
                            }).ConfigureAwait(false);

                    TelemetryHelper.Singleton.LogInfo(
                        "Updated metadata after retries for poison message blob '{0}' - " +
                        "SuccessfulRetryStatus: {1}, FailedDocCount: {2}, SuccesfulRetryCount: {3}",
                        blob.Name,
                        blob.Metadata[EnvironmentConfig.DeadLetterMetaDataSuccessfulRetryStatusKey],
                        failedDocCount,
                        blob.Metadata[EnvironmentConfig.DeadLetterMetaSuccessfulRetryCountKey]);
                }
            }
            catch (Exception error)
            {
                TelemetryHelper.Singleton.LogWarning(
                    "Failed to retry poison messages. Retrying on next iteration... Exception: {0}",
                    error);
            }
            finally
            {
                this.retryConcurrencySemaphore.Release();
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
                        await Task.Delay(SleepTimeInMs);
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
                            this.changeFeedProcessorHosts.TryAdd(
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
                            this.changeFeedProcessorHosts.TryRemove(key, out ChangeFeedProcessorHost removeHost);
                        }
                    }

                    List<Task> retryTasks = new List<Task>();

                    foreach (MigrationConfig configToRetry in configDocs.Where(c => !c.Completed &&
                         (String.IsNullOrWhiteSpace(c.LastPoisonMessageRetryId) ||
                         c.LastPoisonMessageRetryStartedAt < c.PoisonMessageRetryRequestedAt - (long)TimeSpan.FromMinutes(15).TotalMilliseconds )))
                    {
                        configToRetry.LastPoisonMessageRetryId = Guid.NewGuid().ToString("N");
                        configToRetry.LastPoisonMessageRetryStartedAt = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

                        bool ownsRetryForConfig = false;
                        try
                        {
                            await container.ReplaceItemAsync(
                                configToRetry,
                                configToRetry.Id,
                                new PartitionKey(configToRetry.Id),
                                new ItemRequestOptions
                                {
                                    IfMatchEtag = configToRetry.ETag
                                }).ConfigureAwait(false);

                            ownsRetryForConfig = true;
                        }
                        catch (CosmosException error) 
                        {
                            if (error.StatusCode == HttpStatusCode.PreconditionFailed ||
                                error.StatusCode == HttpStatusCode.Conflict)
                            {
                                TelemetryHelper.Singleton.LogInfo(
                                    "Taking ownership of retry fails for config '{0}' - Status Code: {1}",
                                    configToRetry.Id,
                                    error.StatusCode);
                            }
                            else
                            {
                                TelemetryHelper.Singleton.LogWarning(
                                    "Taking ownership of retry fails for config '{0}' - Error: {1}",
                                    configToRetry.Id,
                                    error);
                            }
                        }

                        if (ownsRetryForConfig)
                        {
                            retryTasks.Add(this.RetryPoisonMessages(configToRetry));
                        }
                    }

                    if (retryTasks.Count > 0)
                    {
                        await Task.WhenAll(retryTasks).ConfigureAwait(false);
                    }

                    await Task.Delay(SleepTimeInMs);
                }
            }
        }
    }
}