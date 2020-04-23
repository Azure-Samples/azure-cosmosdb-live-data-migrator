var config = {}

config.host = process.env.cosmosdbaccount || "";
config.authKey = process.env.cosmosdbkey || "";
config.databaseId = process.env.cosmosdbdb || "migrationdetailsdb";
config.collectionId = process.env.cosmosdbcollection || "migrationdetailscoll";
config.sourceClient = null;
config.destClient = null;

module.exports = config;
