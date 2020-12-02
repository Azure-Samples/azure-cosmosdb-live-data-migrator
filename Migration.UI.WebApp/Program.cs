using Azure.Identity;
using Microsoft.ApplicationInsights.Extensibility;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Azure.Cosmos;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Migration.Shared;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Threading.Tasks;

namespace Migration.UI.WebApp
{
    public class Program
    {
        public const string WebAppUserAgentPrefix = "MigrationUI.MigrationMetadata";
        public const string SourceClientUserAgentPrefix = "MigrationUI.Source";
        private static readonly string keyVaultUri = Environment.GetEnvironmentVariable("keyvaulturi");
        private static readonly string migrationMetadataAccount = Environment.GetEnvironmentVariable("cosmosdbaccount");
        private static readonly string jobdb = Environment.GetEnvironmentVariable("cosmosdbdb");
        private static readonly string jobColl = Environment.GetEnvironmentVariable("cosmosdbcollection");
        private static readonly string appInsightsInstrumentationKey =
            Environment.GetEnvironmentVariable("appinsightsinstrumentationkey");
        private static readonly string defaultSourceAccount =
            Environment.GetEnvironmentVariable("defaultsourceaccount");
        private static readonly string defaultDestinationAccount =
            Environment.GetEnvironmentVariable("defaultdestinationaccount");

        public static void Main(string[] args)
        {
            TelemetryConfiguration telemetryConfig = new TelemetryConfiguration(appInsightsInstrumentationKey);
            TelemetryHelper.Initilize(telemetryConfig);

            KeyVaultHelper.Initialize(new Uri(keyVaultUri), new DefaultAzureCredential());

            using (CosmosClient client =
                KeyVaultHelper.Singleton.CreateCosmosClientFromKeyVault(
                    migrationMetadataAccount,
                    WebAppUserAgentPrefix,
                    useBulk: false,
                    retryOn429Forever: true))
            {
                MigrationConfigDal.Initialize(
                    client.GetContainer(jobdb, jobColl),
                    defaultSourceAccount,
                    defaultDestinationAccount);

                CreateHostBuilder(args).Build().Run();
            }
        }

        public static IHostBuilder CreateHostBuilder(string[] args)
        {
            return Host.CreateDefaultBuilder(args)
                .ConfigureWebHostDefaults(webBuilder => webBuilder.UseStartup<Startup>());
        }
    }
}
