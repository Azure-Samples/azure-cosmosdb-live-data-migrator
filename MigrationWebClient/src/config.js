var config = {}

config.host = process.env.cosmosdbaccount || "https://migrationdetails-eastus.documents.azure.com:443/";
config.authKey = process.env.cosmosdbkey || "1mYccQpI3hoLtpBgmzCCLjQOXPnluPSFLxiK5jE3PBJZcrIRH5P47ZlwKmuxiL660k3NVhT5sSAfGtzqqS3lFQ==";
config.databaseId = process.env.cosmosdbdb || "migrationdetailsdb";
config.collectionId = process.env.cosmosdbcollection || "migrationdetailscoll";
config.sourceClient = null;
config.destClient = null;

module.exports = config;
