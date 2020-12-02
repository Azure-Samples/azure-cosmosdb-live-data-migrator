using System;
using System.Collections.Generic;
using System.Globalization;
using System.Text;
using Microsoft.AspNetCore.Authorization;

namespace Migration.UI.WebApp
{
    public class MigrationAppUserRequirement : IAuthorizationRequirement
    {
        private readonly HashSet<string> users;

        public MigrationAppUserRequirement(string tenantId, string allowedUsers)
        {
            if (String.IsNullOrWhiteSpace(tenantId)) { throw new ArgumentNullException(nameof(tenantId)); }
            if (String.IsNullOrWhiteSpace(allowedUsers)) { throw new ArgumentNullException(nameof(allowedUsers)); }

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

        public string TrustedIssuer { get; }

        public ISet<string> AllowedUsers => this.users;
    }
}
