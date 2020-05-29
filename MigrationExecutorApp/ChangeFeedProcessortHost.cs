namespace MigrationConsoleApp
{
    using System;
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
            sourceCollectionClient = new CosmosClient(config.MonitoredUri, config.MonitoredSecretKey);
            destinationCollectionClient = new CosmosClient(config.DestUri, config.DestSecretKey, new CosmosClientOptions() { AllowBulkExecution = true });            
        }

        public CosmosClient GetDestinationCollectionClient()
        {
            if (destinationCollectionClient == null)
            {
                destinationCollectionClient = new CosmosClient(config.DestUri, config.DestSecretKey, new CosmosClientOptions() { AllowBulkExecution = true });
            }
            return destinationCollectionClient;
        }

        public async Task StartAsync()
        {

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
                this.config.LeaseThroughput, "id").Wait();

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
                this.config.DestThroughput, config.TargetPartitionKey).Wait();

            await this.RunChangeFeedHostAsync();
        }

        public async Task CreateCollectionIfNotExistsAsync(string endPointUri, string secretKey, string databaseName, string collectionName, int throughput, string partitionKey)
        {
            Microsoft.Azure.Cosmos.Database db = await destinationCollectionClient.CreateDatabaseIfNotExistsAsync(databaseName);
            containerToStoreDocuments = await db.CreateContainerIfNotExistsAsync(collectionName, "/"+partitionKey);
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
                .GetChangeFeedProcessorBuilder<Document>("Live Data Migrator", ProcessChangesAsync)
                .WithInstanceName("consoleHost")
                .WithLeaseContainer(sourceCollectionClient.GetContainer(config.LeaseDbName, config.LeaseCollectionName))
                .WithStartTime(starttime)               
                .WithMaxItems(1000)
                .Build();

            await changeFeedProcessor.StartAsync().ConfigureAwait(false);

            return changeFeedProcessor;
        }

        async Task ProcessChangesAsync(IReadOnlyCollection<Document> docs, CancellationToken cancellationToken)
        {
            Boolean isSyntheticKey = SourcePartitionKeys.Contains(",");
            Boolean isNestedAttribute = SourcePartitionKeys.Contains("/");
            Container targetContainer = destinationCollectionClient.GetContainer(config.DestDbName, config.DestCollectionName);
            containerToStoreDocuments = targetContainer;
            Document document;
            BulkOperations<Document> bulkOperations = new BulkOperations<Document>(docs.Count);
            foreach (Document doc in docs)
            {
                Console.WriteLine($"\tDetected operation...");
                document = (SourcePartitionKeys != null & TargetPartitionKey != null) ? MapPartitionKey(doc, isSyntheticKey, TargetPartitionKey, isNestedAttribute, SourcePartitionKeys) : document = doc;
                bulkOperations.Tasks.Add(containerToStoreDocuments.CreateItemAsync(item: document, cancellationToken: cancellationToken).CaptureOperationResponse(document));
                await Task.Delay(1);
            }
            BulkOperationResponse<Document> bulkOperationResponse = await bulkOperations.ExecuteAsync();
            if (bulkOperationResponse.Failures.Count > 0 && containerClient != null)
            {
                WriteFailedDocsToBlob("FailedImportDocs", containerClient, bulkOperationResponse);
            }
            LogMetrics(bulkOperationResponse);
        }



        private static void WriteFailedDocsToBlob(string failureType, BlobContainerClient containerClient, BulkOperationResponse<Document> bulkOperationResponse)
        {
            string failedDocs;
            byte[] byteArray;
            BlobClient blobClient = containerClient.GetBlobClient(failureType + Guid.NewGuid().ToString() + ".csv");

            failedDocs = JsonConvert.SerializeObject(String.Join(",", bulkOperationResponse.Failures));
            byteArray = Encoding.ASCII.GetBytes(failureType + ", " + bulkOperationResponse.Failures.Count + "|" + failedDocs);

            using (var ms = new MemoryStream(byteArray))
            {
                blobClient.UploadAsync(ms, overwrite: true);
            }
        }

        private static void LogMetrics(BulkOperationResponse<Document> bulkOperationResponse)
        {
            Program.telemetryClient.TrackMetric("TotalInserted", bulkOperationResponse.SuccessfulDocuments);
            Program.telemetryClient.TrackMetric("InsertedDocuments-Process:"
                + Process.GetCurrentProcess().Id, bulkOperationResponse.SuccessfulDocuments);
            Program.telemetryClient.TrackMetric("TotalRUs", bulkOperationResponse.TotalRequestUnitsConsumed);

            if (bulkOperationResponse.Failures.Count > 0)
            {
                Program.telemetryClient.TrackMetric("BadInputDocsCount", bulkOperationResponse.Failures.Count);
            }

            if (bulkOperationResponse.Failures.Count > 0)
            {
                Program.telemetryClient.TrackMetric("FailedImportDocsCount", bulkOperationResponse.Failures.Count);
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

    public class OperationResponse<T>
    {
        public T Item { get; set; }
        public double RequestUnitsConsumed { get; set; } = 0;
        public bool IsSuccessful { get; set; }
        public Exception CosmosException { get; set; }

    }
    public class BulkOperationResponse<T>
    {
        public TimeSpan TotalTimeTaken { get; set; }
        public int SuccessfulDocuments { get; set; } = 0;
        public double TotalRequestUnitsConsumed { get; set; } = 0;
        public IReadOnlyList<(T, Exception)> Failures { get; set; }
    }
    public class BulkOperations<T>
    {
        public readonly List<Task<OperationResponse<T>>> Tasks;
        private readonly Stopwatch stopwatch = Stopwatch.StartNew();
        public BulkOperations(int operationCount)
        {
            this.Tasks = new List<Task<OperationResponse<T>>>(operationCount);
        }
        public async Task<BulkOperationResponse<T>> ExecuteAsync()
        {
            await Task.WhenAll(this.Tasks);
            this.stopwatch.Stop();
            return new BulkOperationResponse<T>()
            {
                TotalTimeTaken = this.stopwatch.Elapsed,
                TotalRequestUnitsConsumed = this.Tasks.Sum(task => task.Result.RequestUnitsConsumed),
                SuccessfulDocuments = this.Tasks.Count(task => task.Result.IsSuccessful),
                Failures = this.Tasks.Where(task => !task.Result.IsSuccessful).Select(task => (task.Result.Item, task.Result.CosmosException)).ToList()
            };
        }
    }
    static class CaptureOperation
    {
        public static Task<OperationResponse<T>> CaptureOperationResponse<T>(this Task<ItemResponse<T>> task, T item)
        {
            return task.ContinueWith(itemResponse =>
            {
                if (itemResponse.IsCompleted)
                {
                    return new OperationResponse<T>()
                    {
                        Item = item,
                        IsSuccessful = true,
                        RequestUnitsConsumed = task.Result.RequestCharge
                    };
                }

                AggregateException innerExceptions = itemResponse.Exception.Flatten();
                CosmosException cosmosException = innerExceptions.InnerExceptions.FirstOrDefault(innerEx => innerEx is CosmosException) as CosmosException;
                if (cosmosException != null)
                {
                    return new OperationResponse<T>()
                    {
                        Item = item,
                        RequestUnitsConsumed = cosmosException.RequestCharge,
                        IsSuccessful = false,
                        CosmosException = cosmosException
                    };
                }

                return new OperationResponse<T>()
                {
                    Item = item,
                    IsSuccessful = false,
                    CosmosException = innerExceptions.InnerExceptions.FirstOrDefault()
                };
            });
        }
    }

}
