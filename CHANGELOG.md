## Azure Cosmos DB Live Data Migrator - Changelog

<a name="2.0.0"></a>

# 2.0.0 (2020-12-03)

*Features*

* Switched to AAD-based authentication for the Migration client.
* Changed the ARM template to always create a blob storage account for the dead-letter queue (documents that could not be migrated for some reason) - this was optional before - but injected the risk of data loss.

*Bug Fixes*

* Fixed an issue in the 429 retry-policy to make sure for the migration operations we retry more often to avoid failures because of throttling in the new destination.

*Breaking Changes*

* Removed all plain text secrets from any of the meta data documents and configuration. Instead secrets are stored in a key vault and System Managed Identities are used to access the key vault.
* Moved all projects to .Net Core - replaced the Node.js project used for the UI project with Asp.Net Core.
