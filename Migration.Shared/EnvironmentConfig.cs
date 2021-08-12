using System;
using System.Text.RegularExpressions;

namespace Migration.Shared
{
    public class EnvironmentConfig
    {
        public const string DeadLetterMetaDataSuccessfulRetryStatusKey = "SuccesfulRetryStatus";
        public const string DeadLetterMetaSuccessfulRetryCountKey = "SuccesfulRetryCount";
        public const string FailedDocSeperator = "_(@)_";
        public const string FailureColumnSeperator = "_-|-_";
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