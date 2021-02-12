## Azure Cosmos DB Live Data Migrator - Changelog

<a name="2.0.0"></a>



# 2.0.1 (2021-02-11)

*Features*

* Option to create a migration to only insert missing documents. This can be useful if documents have been written to the poison message location initially to reprocess just documents that don't exist in the destination yet.

*Bug Fixes*

* The Monitoring WebApp calculates the number of documents by running a query. Especially for large containers this is less efficient than retrieving the quota info from the Container metadata. So using that approach going forward.



# 2.0.0 (2020-12-04)

*Features*

* Switched to AAD-based authentication for the Migration client.
* Changed the ARM template to always create a blob storage account for the dead-letter queue (documents that could not be migrated for some reason) - this was optional before - but injected the risk of data loss.

*Bug Fixes*

* Fixed an issue in the 429 retry-policy to make sure for the migration operations we retry more often to avoid failures because of throttling in the new destination.

*Breaking Changes*

* Removed all plain text secrets from any of the meta data documents and configuration. Instead secrets are stored in a key vault and System Managed Identities are used to access the key vault.
* Moved all projects to .Net Core - replaced the Node.js project used for the UI project with Asp.Net Core.
