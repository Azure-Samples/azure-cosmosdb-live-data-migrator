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

        public static void Main(string[] args)
        {
            EnvironmentConfig.Initialize();

            _ = EnvironmentConfig.Singleton.TenantId;
            _ = EnvironmentConfig.Singleton.AllowedUsers;

            TelemetryConfiguration telemetryConfig = new TelemetryConfiguration(
                EnvironmentConfig.Singleton.AppInsightsInstrumentationKey);
            TelemetryHelper.Initilize(telemetryConfig);

            KeyVaultHelper.Initialize(new Uri(EnvironmentConfig.Singleton.KeyVaultUri), new DefaultAzureCredential());

            using (CosmosClient client =
                KeyVaultHelper.Singleton.CreateCosmosClientFromKeyVault(
                    EnvironmentConfig.Singleton.MigrationMetadataCosmosAccountName,
                    WebAppUserAgentPrefix,
                    useBulk: false,
                    retryOn429Forever: true))
            {
                MigrationConfigDal.Initialize(
                    client.GetContainer(
                        EnvironmentConfig.Singleton.MigrationMetadataDatabaseName,
                        EnvironmentConfig.Singleton.MigrationMetadataContainerName),
                    EnvironmentConfig.Singleton.DefaultSourceAccount,
                    EnvironmentConfig.Singleton.DefaultDestinationAccount);

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
