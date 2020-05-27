namespace MigrationConsoleApp
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.IO;
    using System.Linq;
    using System.Runtime.Serialization.Json;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using System.Xml.Linq;
    using System.Xml.XPath;
    using Azure.Storage.Blobs;
    using Microsoft.Azure.Cosmos;
    using Microsoft.Azure.Cosmos.Fluent;
    using Microsoft.Azure.Documents;
    using Newtonsoft.Json;


    public class ChangeFeedProcessorHost
    {
        private MigrationConfig config;
        private ChangeFeedProcessor changeFeedProcessor;
        private static CosmosClient destinationCollectionClient;
        private static CosmosClient sourceCollectionClient;
        private static Container containerToStoreDocuments;
        private readonly string SourcePartitionKeys;
        private readonly string TargetPartitionKey;
        BlobContainerClient containerClient = null;


        public ChangeFeedProcessorHost(MigrationConfig config)
        {
            this.config = config;
            SourcePartitionKeys = config.SourcePartitionKeys;
            TargetPartitionKey = config.TargetPartitionKey;
            destinationCollectionClient = GetCustomClient("AccountEndpoint=" + config.DestUri + ";AccountKey=" + config.DestSecretKey);
            //destinationCollectionClient = GetCustomClient("AccountEndpoint=https://tvk-sqlapi.documents.azure.com:443/;AccountKey=YdtPFdcmsOTSrPzyobK2tqRJzDmQwkn4EDsBaZimDZwiwGP2qsc8YAEMDF3xk0NDwf0acP4cvwHgJrXSJ6kU8A==;");

            sourceCollectionClient = GetCustomClient("AccountEndpoint=" + config.MonitoredUri + ";AccountKey=" + config.MonitoredSecretKey);
            //sourceCollectionClient = GetCustomClient("AccountEndpoint=https://tvk-sqlapi.documents.azure.com:443/;AccountKey=YdtPFdcmsOTSrPzyobK2tqRJzDmQwkn4EDsBaZimDZwiwGP2qsc8YAEMDF3xk0NDwf0acP4cvwHgJrXSJ6kU8A==;");
        }

        public CosmosClient GetDestinationCollectionClient()
        {
            if (destinationCollectionClient == null)
            {
                //AccountEndpoint=https://tvk-sqlapi.documents.azure.com:443/;AccountKey=YdtPFdcmsOTSrPzyobK2tqRJzDmQwkn4EDsBaZimDZwiwGP2qsc8YAEMDF3xk0NDwf0acP4cvwHgJrXSJ6kU8A==;
                destinationCollectionClient = GetCustomClient("AccountEndpoint=" + config.DestUri + ";AccountKey=" + config.DestSecretKey);
                //destinationCollectionClient = GetCustomClient("AccountEndpoint=https://tvk-sqlapi.documents.azure.com:443/;AccountKey=YdtPFdcmsOTSrPzyobK2tqRJzDmQwkn4EDsBaZimDZwiwGP2qsc8YAEMDF3xk0NDwf0acP4cvwHgJrXSJ6kU8A==;");
                
                //sourceCollectionClient = GetCustomClient("AccountEndpoint=" + config.MonitoredUri + ";AccountKey=" + config.MonitoredSecretKey);
                sourceCollectionClient = GetCustomClient("AccountEndpoint=https://tvk-sqlapi.documents.azure.com:443/;AccountKey=YdtPFdcmsOTSrPzyobK2tqRJzDmQwkn4EDsBaZimDZwiwGP2qsc8YAEMDF3xk0NDwf0acP4cvwHgJrXSJ6kU8A==;");
            }

            return destinationCollectionClient;
        }

        private static CosmosClient GetCustomClient(string connectionString)
        {
            CosmosClientBuilder builder = new CosmosClientBuilder(connectionString)
                .WithApplicationName("CosmosFunctionsMigration")
                .WithBulkExecution(true)
                .WithThrottlingRetryOptions(TimeSpan.FromSeconds(30), 10);

            return builder.Build();
        }

        public async Task StartAsync()
        {
            Trace.TraceInformation(
                "Starting monitor(source) collection creation: Url {0} - key {1} - dbName {2} - collectionName {3}",
                this.config.MonitoredUri,
                this.config.MonitoredSecretKey,
                this.config.MonitoredDbName,
                this.config.MonitoredCollectionName);

            this.CreateCollectionIfNotExistsAsync(
               this.config.MonitoredUri,
               this.config.MonitoredSecretKey,
               this.config.MonitoredDbName,
               this.config.MonitoredCollectionName,
               this.config.MonitoredThroughput).Wait();

            Trace.TraceInformation(
                "Starting lease (transaction log of change feed) standard collection creation: Url {0} - key {1} - dbName {2} - collectionName {3}",
                this.config.LeaseUri,
                this.config.LeaseSecretKey,
                this.config.LeaseDbName,
                this.config.LeaseCollectionName);

            this.CreateCollectionIfNotExistsAsync(
                this.config.LeaseUri,
                this.config.LeaseSecretKey,
                this.config.LeaseDbName,
                this.config.LeaseCollectionName,
                this.config.LeaseThroughput).Wait();

            Trace.TraceInformation(
                "destination (sink) collection : Url {0} - key {1} - dbName {2} - collectionName {3}",
                this.config.DestUri,
                this.config.DestSecretKey,
                this.config.DestDbName,
                this.config.DestCollectionName);

            this.CreateCollectionIfNotExistsAsync(
                this.config.DestUri,
                this.config.DestSecretKey,
                this.config.DestDbName,
                this.config.DestCollectionName,
                this.config.DestThroughput).Wait();

            await this.RunChangeFeedHostAsync();
        }

        public async Task CreateCollectionIfNotExistsAsync(string endPointUri, string secretKey, string databaseName, string collectionName, int throughput)
        {
            //using (CosmosClient client = GetCustomClient("AccountEndpoint=" + endPointUri + ";AccountKey=" + secretKey))
            using (CosmosClient client = GetCustomClient("AccountEndpoint=https://tvk-sqlapi.documents.azure.com:443/;AccountKey=YdtPFdcmsOTSrPzyobK2tqRJzDmQwkn4EDsBaZimDZwiwGP2qsc8YAEMDF3xk0NDwf0acP4cvwHgJrXSJ6kU8A==;"))   
            {
                await client.CreateDatabaseIfNotExistsAsync(databaseName);
            }
        }

        public async Task CloseAsync()
        {
            if(GetDestinationCollectionClient() != null )
            {
                GetDestinationCollectionClient().Dispose();
            }

            if(changeFeedProcessor != null)
            {
                await changeFeedProcessor.StopAsync();
            }

            destinationCollectionClient = null;
            changeFeedProcessor = null;
        }

        public async Task<ChangeFeedProcessor> RunChangeFeedHostAsync()
        {
            string hostName = Guid.NewGuid().ToString();
            Trace.TraceInformation("Host name {0}", hostName);


            var docTransformer = new DefaultDocumentTransformer();
          
            if (!String.IsNullOrEmpty(config.BlobConnectionString))
            {
                BlobServiceClient blobServiceClient = new BlobServiceClient(config.BlobConnectionString);
                containerClient = blobServiceClient.GetBlobContainerClient(config.BlobContainerName);
                await containerClient.CreateIfNotExistsAsync();
            }

            DateTime starttime = DateTime.MinValue.ToUniversalTime();
            if (config.DataAgeInHours.HasValue)
            {
                if (config.DataAgeInHours.Value >= 0)
                {
                    starttime = DateTime.UtcNow.Subtract(TimeSpan.FromHours(config.DataAgeInHours.Value));
                }
            }

            changeFeedProcessor = sourceCollectionClient.GetContainer(config.MonitoredDbName, config.MonitoredCollectionName)
                .GetChangeFeedProcessorBuilder<Document>("Live Data Migrator", HandleChangesAsync)
                .WithInstanceName("consoleHost")
                .WithLeaseContainer(sourceCollectionClient.GetContainer(config.LeaseDbName, config.LeaseCollectionName))
                .WithStartTime(starttime)               
                .WithMaxItems(1000)
                .Build();

            await changeFeedProcessor.StartAsync().ConfigureAwait(false);

            return changeFeedProcessor;
        }

        async Task HandleChangesAsync(IReadOnlyCollection<Document> docs, CancellationToken cancellationToken)
        {
            Boolean isSyntheticKey = SourcePartitionKeys.Contains(",");
            Boolean isNestedAttribute = SourcePartitionKeys.Contains("/");
            Container targetContainer = destinationCollectionClient.GetContainer(config.DestDbName, config.DestCollectionName);
            containerToStoreDocuments = targetContainer;
            ConcurrentDictionary<int, int> failedMetrics = new ConcurrentDictionary<int, int>();
            List<Task> tasks = new List<Task>();
            List<Document> failedDocs = new List<Document>();
            Stopwatch stopwatch = Stopwatch.StartNew();
            Document document = new Document();
            foreach (Document doc in docs)
            {
                Console.WriteLine($"\tDetected operation...");
                document = (SourcePartitionKeys != null & TargetPartitionKey != null) ? MapPartitionKey(doc, isSyntheticKey, TargetPartitionKey, isNestedAttribute, SourcePartitionKeys) : document = doc;
                tasks.Add(containerToStoreDocuments.CreateItemAsync(item: document, cancellationToken: cancellationToken).ContinueWith((Task<ItemResponse<Document>> task) =>
                {
                    AggregateException innerExceptions = task.Exception.Flatten();
                    CosmosException cosmosException = (CosmosException)innerExceptions.InnerExceptions.FirstOrDefault(innerEx => innerEx is CosmosException);
                    failedMetrics.AddOrUpdate((int)cosmosException.StatusCode, 0, (key, value) => value + 1);
                    WriteFailedDocsToBlob("FailedDocImport", containerClient, cosmosException, doc);
                }, TaskContinuationOptions.OnlyOnFaulted));
                // Simulate work
                await Task.Delay(1);
            }
        }

        private static void WriteFailedDocsToBlob(string failureType, BlobContainerClient containerClient, CosmosException cosmosException, Document doc)
        {
            string failedDoc;
            byte[] byteArray;
            BlobClient blobClient = containerClient.GetBlobClient(failureType + Guid.NewGuid().ToString() + ".csv");

            failedDoc = JsonConvert.SerializeObject(String.Join(",", doc));
            byteArray = Encoding.ASCII.GetBytes(failureType + ", " + cosmosException + "|" + failedDoc);
      
            using (var ms = new MemoryStream(byteArray))
            {
                blobClient.UploadAsync(ms, overwrite: true);
            }
        }

        public static Document MapPartitionKey(Document doc, Boolean isSyntheticKey, string targetPartitionKey, Boolean isNestedAttribute, string sourcePartitionKeys)
        {
            if (isSyntheticKey)
            {
                doc = CreateSyntheticKey(doc, sourcePartitionKeys, isNestedAttribute, targetPartitionKey);
            }
            else
            {
                doc.SetPropertyValue(targetPartitionKey, isNestedAttribute == true ? GetNestedValue(doc, sourcePartitionKeys) : doc.GetPropertyValue<string>(sourcePartitionKeys));
            }
            return doc;
        }

        public static Document CreateSyntheticKey(Document doc, string sourcePartitionKeys, Boolean isNestedAttribute, string targetPartitionKey)
        {
            StringBuilder syntheticKey = new StringBuilder();
            string[] sourceAttributeArray = sourcePartitionKeys.Split(',');
            int arraylength = sourceAttributeArray.Length;
            int count = 1;
            foreach (string rawattribute in sourceAttributeArray)
            {
                string attribute = rawattribute.Trim();
                if (count == arraylength)
                {
                    string val = isNestedAttribute == true ? GetNestedValue(doc, attribute) : doc.GetPropertyValue<string>(attribute);
                    syntheticKey.Append(val);
                }
                else
                {
                    string val = isNestedAttribute == true ? GetNestedValue(doc, attribute) + "-" : doc.GetPropertyValue<string>(attribute) + "-";
                    syntheticKey.Append(val);
                }
                count++;
            }
            doc.SetPropertyValue(targetPartitionKey, syntheticKey.ToString());
            return doc;
        }

        public static string GetNestedValue(Document doc, string path)
        {
            var jsonReader = JsonReaderWriterFactory.CreateJsonReader(Encoding.UTF8.GetBytes(doc.ToString()), new System.Xml.XmlDictionaryReaderQuotas());
            var root = XElement.Load(jsonReader);
            string value = root.XPathSelectElement("//" + path).Value;
            return value;
        }

    }

}
