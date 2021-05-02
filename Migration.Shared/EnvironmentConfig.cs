using System;

namespace Migration.Shared
{
    public class EnvironmentConfig
    {
        private static EnvironmentConfig singletonInstance;

        private readonly Lazy<string> tenantId;
        private readonly Lazy<string> allowedUsers;

        private EnvironmentConfig()
        {
            this.KeyVaultUri = GetRequiredEnvironmentVariable("keyvaulturi");
            this.MigrationMetadataCosmosAccountName = GetRequiredEnvironmentVariable("cosmosdbaccount");
            this.DeadLetterAccountName = GetRequiredEnvironmentVariable("deadletteraccount");
            this.MigrationMetadataDatabaseName = GetRequiredEnvironmentVariable("cosmosdbdb");
            this.MigrationMetadataContainerName = GetRequiredEnvironmentVariable("cosmosdbcollection");
            this.MigrationLeasesContainerName = GetRequiredEnvironmentVariable("cosmosdbleasescollection");
            this.AppInsightsInstrumentationKey = GetRequiredEnvironmentVariable("appinsightsinstrumentationkey");
            this.DefaultSourceAccount = Environment.GetEnvironmentVariable("defaultsourceaccount");
            this.DefaultDestinationAccount = Environment.GetEnvironmentVariable("defaultdestinationaccount");
            this.tenantId = new Lazy<string>(() => GetRequiredEnvironmentVariable("tenantid"));
            this.allowedUsers = new Lazy<string>(() => GetRequiredEnvironmentVariable("allowedusers"));

            //this.KeyVaultUri = "https://thvankralivemigrateakv.vault.azure.net/";
            //this.MigrationMetadataCosmosAccountName = "metadata-cdb";
            //this.DeadLetterAccountName = "thvankralivemigratorstor2";
            //this.MigrationMetadataDatabaseName = "MigrationServiceDB";
            //this.MigrationMetadataContainerName = "MigrationStatus";
            //this.MigrationLeasesContainerName = "Leases";
            //this.AppInsightsInstrumentationKey = "bt80eho3ezvp5nsnj67t747ywt4snrzm4ixoi16p";
            //this.DefaultSourceAccount = "tvk-sqlapi";
            //this.DefaultDestinationAccount = "tvk-sqlapi";
            //this.tenantId = new Lazy<string>(() => "72f988bf-86f1-41af-91ab-2d7cd011db47");
            //this.allowedUsers = new Lazy<string>(() => "thvankra@microsoft.com|fabianm@microsoft.com");
        }

        public static EnvironmentConfig Singleton
        {
            get
            {
                if (singletonInstance == null)
                {
                    throw new InvalidOperationException("EnvironmentConfig has not yet been initialized.");
                }

                return singletonInstance;
            }
        }

        public static void Initialize()
        {
            singletonInstance = new EnvironmentConfig();
        }

        public string KeyVaultUri { get; }
        public string MigrationMetadataCosmosAccountName { get; }
        public string DeadLetterAccountName { get; }
        public string MigrationMetadataDatabaseName { get; }
        public string MigrationMetadataContainerName { get; }
        public string MigrationLeasesContainerName { get; }
        public string AppInsightsInstrumentationKey { get; }
        public string DefaultSourceAccount { get; }
        public string DefaultDestinationAccount { get; }
        public string TenantId => this.tenantId.Value;
        public string AllowedUsers => this.allowedUsers.Value;

        private static string GetRequiredEnvironmentVariable(string name)
        {
            return Environment.GetEnvironmentVariable(name) ?? throw new ArgumentNullException(
                nameof(name),
                "Environment variable '{0}' has not been defined.");
        }
    }
}