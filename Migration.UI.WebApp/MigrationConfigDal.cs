using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Microsoft.Azure.Cosmos;
using Migration.Shared;
using Migration.Shared.DataContracts;

namespace Migration.UI.WebApp
{
    public class MigrationConfigDal
    {
        private static MigrationConfigDal singletonInstance;
        private readonly Container container;

        private MigrationConfigDal(
            Container container,
            string defaultSourceAccount,
            string defaultDestinationAccount)
        {
            this.container = container;
            this.DefaultSourceAccount = defaultSourceAccount;
            this.DefaultDestinationAccount = defaultDestinationAccount;
        }

        public static MigrationConfigDal Singleton
        {
            get
            {
                if (singletonInstance == null)
                {
                    throw new InvalidOperationException("MigrationConfigDal has not yet been initialized.");
                }

                return singletonInstance;
            }
        }

        public string DefaultSourceAccount { get; }

        public string DefaultDestinationAccount { get; }

        public static void Initialize(
            Container migrationStatusContainer,
            string defaultSourceAccount,
            string defaultDestinationAccount)
        {
            if (migrationStatusContainer == null)
            {
                throw new ArgumentNullException(nameof(migrationStatusContainer));
            }

            singletonInstance = new MigrationConfigDal(
                migrationStatusContainer,
                defaultSourceAccount,
                defaultDestinationAccount);
        }

        public async Task<List<MigrationConfig>> GetActiveMigrationsAsync()
        {
            try
            {
                FeedResponse<MigrationConfig> response = await this.container
                          .GetItemQueryIterator<MigrationConfig>("select * from c where NOT c.completed")
                          .ReadNextAsync()
                          .ConfigureAwait(false);

                return response.AsEnumerable<MigrationConfig>().ToList();
            }
            catch (Exception error)
            {
                TelemetryHelper.Singleton.LogError(
                    "MigrationConfigDal.GetActiveMigrationsAsync failed: {0}",
                    error);

                throw;
            }
        }

        public async Task<MigrationConfig> GetMigrationAsync(string id)
        {
            try
            {
                return await this.container
                    .ReadItemAsync<MigrationConfig>(id, new PartitionKey(id))
                    .ConfigureAwait(false);
            }
            catch (Exception error)
            {
                TelemetryHelper.Singleton.LogError(
                    "MigrationConfigDal.GetMigrationAsync({0}) failed: {1}",
                    id,
                    error);

                throw;
            }
        }

        public async Task<MigrationConfig> CreateMigrationAsync(MigrationConfig config)
        {
            if (config == null) { throw new ArgumentNullException(nameof(config)); }

            if (String.IsNullOrWhiteSpace(config.Id))
            {
                config.Id = Guid.NewGuid().ToString("N");
            }

            try
            {
                BlobContainerClient deadletterClient = KeyVaultHelper.Singleton.GetBlobContainerClientFromKeyVault(
                    EnvironmentConfig.Singleton.DeadLetterAccountName,
                    config.Id?.ToLowerInvariant().Replace("-", String.Empty));
                await deadletterClient
                    .CreateIfNotExistsAsync(PublicAccessType.None)
                    .ConfigureAwait(false);

                return await this.container
                    .CreateItemAsync<MigrationConfig>(config, new PartitionKey(config.Id))
                    .ConfigureAwait(false);
            }
            catch (Exception error)
            {
                TelemetryHelper.Singleton.LogError(
                    "MigrationConfigDal.CreateMigrationAsync for document with id {0} failed: {1}",
                    config.Id,
                    error);

                throw;
            }
        }

        public async Task<MigrationConfig> CompleteMigrationAsync(string id)
        {
            try
            {
                while (true)
                {
                    MigrationConfig config = await this.GetMigrationAsync(id).ConfigureAwait(false);

                    if (config.Completed)
                    {
                        return config;
                    }

                    config.Completed = true;

                    try
                    {
                        return await this.container
                            .ReplaceItemAsync<MigrationConfig>(
                                config,
                                config.Id,
                                new PartitionKey(config.Id),
                                new ItemRequestOptions
                                {
                                    IfMatchEtag = config.ETag,
                                })
                            .ConfigureAwait(false);
                    }
                    catch (CosmosException error)
                    {
                        if (error.StatusCode == HttpStatusCode.PreconditionFailed)
                        {
                            continue;
                        }
                    }
                }
            }
            catch (Exception error)
            {
                TelemetryHelper.Singleton.LogError(
                    "MigrationConfigDal.CompleteMigrationAsync({0}) failed: {1}",
                    id,
                    error);

                throw;
            }
        }

        public async Task<MigrationConfig> RetryPosionMsgsAsync(string id)
        {
            try
            {
                while (true)
                {
                    MigrationConfig config = await this.GetMigrationAsync(id).ConfigureAwait(false);

                    if (config.Completed)
                    {
                        return config;
                    }

                    config.PoisonMessageRetryRequestedAt = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

                    try
                    {
                        return await this.container
                            .ReplaceItemAsync<MigrationConfig>(
                                config,
                                config.Id,
                                new PartitionKey(config.Id),
                                new ItemRequestOptions
                                {
                                    IfMatchEtag = config.ETag,
                                })
                            .ConfigureAwait(false);
                    }
                    catch (CosmosException error)
                    {
                        if (error.StatusCode == HttpStatusCode.PreconditionFailed)
                        {
                            continue;
                        }
                    }
                }
            }
            catch (Exception error)
            {
                TelemetryHelper.Singleton.LogError(
                    "MigrationConfigDal.RetryPosionMsgsAsync({0}) failed: {1}",
                    id,
                    error);

                throw;
            }
        }
    }
}