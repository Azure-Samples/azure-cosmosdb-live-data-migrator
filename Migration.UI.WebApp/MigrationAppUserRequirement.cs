using System;
using System.Collections.Generic;
using System.Globalization;
using System.Text;
using Microsoft.AspNetCore.Authorization;
using Migration.Shared;

namespace Migration.UI.WebApp
{
    public class MigrationAppUserRequirement : IAuthorizationRequirement
    {
        private readonly HashSet<string> users;

        public MigrationAppUserRequirement(string tenantId, string allowedUsers)
        {
            if (String.IsNullOrWhiteSpace(tenantId)) { throw new ArgumentNullException(nameof(tenantId)); }
            if (String.IsNullOrWhiteSpace(allowedUsers)) { throw new ArgumentNullException(nameof(allowedUsers)); }

            try
            {
                this.TrustedIssuer =
                    String.Format(CultureInfo.InvariantCulture, "https://sts.windows.net/{0}/", tenantId);
                this.users = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                foreach (string user in allowedUsers.Split('|'))
                {
                    if (String.IsNullOrWhiteSpace(user))
                    {
                        continue;
                    }

                    this.users.Add(user.Trim());
                }
            }
            catch (Exception error)
            {
                TelemetryHelper.Singleton.LogError(
                    "Initialization of the {0} used for authorization failed. Provided " +
                        "tenantId '{1}' or allowed users list '{2}' are most likely incorrect. {3}",
                    nameof(MigrationAppUserRequirement),
                    tenantId,
                    allowedUsers,
                    error);

                throw;
            }
        }

        public string TrustedIssuer { get; }

        public ISet<string> AllowedUsers => this.users;
    }
}
