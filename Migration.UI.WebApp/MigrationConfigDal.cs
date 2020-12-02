using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Microsoft.Azure.Cosmos;
using Migration.Shared;
using Migration.Shared.DataContracts;

namespace Migration.UI.WebApp
{
    public class MigrationConfigDal
    {
        private static MigrationConfigDal singletonInstance;

        private readonly Container container;

        private readonly CosmosClient defaultSourceClient;

        private List<string> defaultSourceDatabases;

        private MigrationConfigDal(
            Container container,
            string defaultSourceAccount,
            string defaultDestinationAccount)
        {
            this.container = container;
            this.DefaultSourceAccount = defaultSourceAccount;
            this.DefaultDestinationAccount = defaultDestinationAccount;

            if (!string.IsNullOrWhiteSpace(defaultSourceAccount))
            {
                this.defaultSourceClient = KeyVaultHelper.Singleton.CreateCosmosClientFromKeyVault(
                    defaultSourceAccount,
                    Program.SourceClientUserAgentPrefix,
                    useBulk: false,
                    retryOn429Forever: false);
            }
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
            FeedResponse<MigrationConfig> response = await this.container
                      .GetItemQueryIterator<MigrationConfig>("select * from c where NOT c.completed")
                      .ReadNextAsync()
                      .ConfigureAwait(false);

            return response.AsEnumerable<MigrationConfig>().ToList();
        }

        public async Task<MigrationConfig> GetMigrationAsync(string id)
        {
            return await this.container
                .ReadItemAsync<MigrationConfig>(id, new PartitionKey(id))
                .ConfigureAwait(false);
        }

        public async Task<MigrationConfig> CreateMigrationAsync(MigrationConfig config)
        {
            return await this.container
                .CreateItemAsync<MigrationConfig>(config, new PartitionKey(config.Id))
                .ConfigureAwait(false);
        }

        public async Task<MigrationConfig> CompleteMigrationAsync(string id)
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

        public async Task<IList<string>> GetTermsOfDefaultSource()
        {
            if (this.defaultSourceDatabases != null)
            {
                return this.defaultSourceDatabases;
            }

            if (this.defaultSourceClient == null)
            {
                return this.defaultSourceDatabases = new List<string>();
            }

            HashSet<string> sourceDatabases = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

            FeedIterator<string> iterator = 
                this.defaultSourceClient.GetDatabaseQueryIterator<string>("select VALUE(c.id) from c");
            while (iterator.HasMoreResults)
            {
                FeedResponse<string> response = await iterator.ReadNextAsync().ConfigureAwait(false);
                foreach (string database in response)
                {
                    sourceDatabases.Add(database);
                }
            }

            HashSet<string> terms = new HashSet<string>(sourceDatabases, StringComparer.OrdinalIgnoreCase);
            foreach (string dbName in sourceDatabases)
            {
                Database db = this.defaultSourceClient.GetDatabase(dbName);

                FeedIterator<string> containerIterator =
                    db.GetContainerQueryIterator<string>("select VALUE(ci.id) from c");
                while (containerIterator.HasMoreResults)
                {
                    FeedResponse<string> response = await containerIterator.ReadNextAsync().ConfigureAwait(false);
                    foreach (string container in response)
                    {
                        terms.Add(container);
                    }
                }
            }

            List<string> results = terms.ToList();
            results.Sort();

            return this.defaultSourceDatabases = results;
        }
    }
}
