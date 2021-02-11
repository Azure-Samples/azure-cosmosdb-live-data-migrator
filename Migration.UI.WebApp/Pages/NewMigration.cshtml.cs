using System;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.RazorPages;
using Migration.Shared.DataContracts;

namespace Migration.UI.WebApp.Pages
{
    public class NewMigrationModel : PageModel
    {
        public async Task<IActionResult> OnPostSubmitAsync(
            string id,
            string sourceAccount,
            string sourceDatabase,
            string sourceContainer,
            string sourcePK,
            string destinationAccount,
            string destinationDatabase,
            string destinationContainer,
            string destinationPK,
            bool onlyMissingDocuments)
        {
            MigrationConfig newConfig = new MigrationConfig
            {
                Id = id != null && id.StartsWith("/") ? id[1..] : id,
                MonitoredAccount = sourceAccount,
                MonitoredDbName = sourceDatabase,
                MonitoredCollectionName = sourceContainer,
                SourcePartitionKeys = sourcePK,
                DestAccount = destinationAccount,
                DestDbName = destinationDatabase,
                DestCollectionName = destinationContainer,
                TargetPartitionKey = destinationPK,
                Completed = false,
                StartTimeEpochMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                OnlyInsertMissingItems = onlyMissingDocuments,
            };

            await MigrationConfigDal.Singleton
                .CreateMigrationAsync(newConfig)
                .ConfigureAwait(false);

            return this.RedirectToPage("/Migrations", null);
        }
    }
}