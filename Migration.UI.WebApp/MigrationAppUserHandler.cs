using System;
using System.Collections.Generic;
using System.Security.Claims;
using System.Text;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Authorization;
using Migration.Shared;

namespace Migration.UI.WebApp
{
    public class MigrationAppUserHandler : AuthorizationHandler<MigrationAppUserRequirement>
    {
        public static readonly HashSet<string> validClaimTypes = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
        {
            "http://schemas.microsoft.com/identity/claims/objectidentifier",
            ClaimTypes.Email,
            ClaimTypes.Upn,
            ClaimTypes.Name
        };

        protected override Task HandleRequirementAsync(
            AuthorizationHandlerContext context,
            MigrationAppUserRequirement requirement)
        {
            if (context == null) { throw new ArgumentNullException(nameof(context)); }
            if (requirement == null) { throw new ArgumentNullException(nameof(requirement)); }

            try
            {
                if (context.User.HasClaim(c =>
                    String.Equals(requirement.TrustedIssuer, c.Issuer) &&
                    requirement.AllowedUsers.Contains(c.Value) &&
                    validClaimTypes.Contains(c.Type)))
                {
                    context.Succeed(requirement);
                }

                return Task.CompletedTask;
            }
            catch (Exception error)
            {
                TelemetryHelper.Singleton.LogError(
                    "Processing custom authorization in {0} failed. {1}",
                    nameof(MigrationAppUserHandler),
                    error);

                throw;
            }
        }
    }
}
