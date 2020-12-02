using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.RazorPages;
using Microsoft.Extensions.Logging;
using Migration.Shared.DataContracts;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Migration.UI.WebApp.Pages
{
    public class MigrationsModel : PageModel
    {
        private readonly ILogger<MigrationsModel> _logger;
        public readonly List<MigrationConfig> records = new List<MigrationConfig>();

        public MigrationsModel(ILogger<MigrationsModel> logger)
        {
            this._logger = logger;
        }

        public async Task OnGetAsync()
        {
            this.records.AddRange(
                await MigrationConfigDal.Singleton.GetActiveMigrationsAsync().ConfigureAwait(false));
        }

        public async Task<IActionResult> OnPostCompleteAsync(
            string id)
        {
            await MigrationConfigDal.Singleton
                .CompleteMigrationAsync(id)
                .ConfigureAwait(false);

            return this.RedirectToPage("/Migrations", null);
        }
    }
}
