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
    using Microsoft.Azure.CosmosDB.BulkExecutor;
    using Microsoft.Azure.CosmosDB.BulkExecutor.BulkImport;
    using Microsoft.Azure.Documents;
    using Microsoft.Azure.Documents.ChangeFeedProcessor;
    using Microsoft.Azure.Documents.ChangeFeedProcessor.FeedProcessing;
    using Microsoft.Azure.Documents.Client;
    using Newtonsoft.Json;
    using ChangeFeedObserverCloseReason = Microsoft.Azure.Documents.ChangeFeedProcessor.FeedProcessing.ChangeFeedObserverCloseReason;
    using IChangeFeedObserver = Microsoft.Azure.Documents.ChangeFeedProcessor.FeedProcessing.IChangeFeedObserver;

    public class DocumentFeedObserver: IChangeFeedObserver
    {
        private readonly DocumentClient client;
        private readonly Uri destinationCollectionUri;
        private IBulkExecutor bulkExecutor;
        private IDocumentTransformer documentTransformer;
        private BlobContainerClient containerClient;
        private readonly string SourcePartitionKeys;
        private readonly string TargetPartitionKey;

        public DocumentFeedObserver(string SourcePartitionKeys, string TargetPartitionKey, DocumentClient client, DocumentCollectionInfo destCollInfo, IDocumentTransformer documentTransformer, BlobContainerClient containerClient)
        {
            this.SourcePartitionKeys = SourcePartitionKeys;
            this.TargetPartitionKey = TargetPartitionKey;
            this.client = client;
            this.destinationCollectionUri = UriFactory.CreateDocumentCollectionUri(destCollInfo.DatabaseName, destCollInfo.CollectionName);
            this.documentTransformer = documentTransformer;
            this.containerClient = containerClient;  
        }

        public async Task OpenAsync(IChangeFeedObserverContext context)
        {
            client.ConnectionPolicy.RetryOptions.MaxRetryWaitTimeInSeconds = 100000;
            client.ConnectionPolicy.RetryOptions.MaxRetryAttemptsOnThrottledRequests = 100000;

            DocumentCollection destinationCollection = await client.ReadDocumentCollectionAsync(this.destinationCollectionUri);
            bulkExecutor = new BulkExecutor(client, destinationCollection);
            client.ConnectionPolicy.UserAgentSuffix = (" migrationService");

            Console.ForegroundColor = ConsoleColor.Magenta;
            Console.WriteLine("Observer opened for partition Key Range: {0}", context.PartitionKeyRangeId);
            Console.ForegroundColor = ConsoleColor.White;

            await bulkExecutor.InitializeAsync();
        }

        public Task CloseAsync(IChangeFeedObserverContext context, ChangeFeedObserverCloseReason reason)
        {
            Console.ForegroundColor = ConsoleColor.Cyan;
            Console.WriteLine("Observer closed, {0}", context.PartitionKeyRangeId);
            Console.WriteLine("Reason for shutdown, {0}", reason);
            Console.ForegroundColor = ConsoleColor.White;

            return Task.CompletedTask;
        }

        public async Task ProcessChangesAsync(
            IChangeFeedObserverContext context, 
            IReadOnlyList<Document> docs, 
            CancellationToken cancellationToken)
        {
            BulkImportResponse bulkImportResponse = new BulkImportResponse();
            try
            {
                Boolean isSyntheticKey = SourcePartitionKeys.Contains(",");
                Boolean isNestedAttribute = SourcePartitionKeys.Contains("/");

                List<Document> transformedDocs = new List<Document>();
                Document document = new Document();
                foreach (var doc in docs)
                {
                    document = (SourcePartitionKeys != null & TargetPartitionKey != null) ? MapPartitionKey(doc, isSyntheticKey, TargetPartitionKey, isNestedAttribute, SourcePartitionKeys) : document = doc;
                    transformedDocs.AddRange(documentTransformer.TransformDocument(document).Result);
                }

                bulkImportResponse = await bulkExecutor.BulkImportAsync(
                    documents: transformedDocs,
                    enableUpsert: true,
                    maxConcurrencyPerPartitionKeyRange: 1,
                    disableAutomaticIdGeneration: true,
                    maxInMemorySortingBatchSize: null,
                    cancellationToken: new CancellationToken(),
                    maxMiniBatchSizeBytes: 100 * 1024);

                if (bulkImportResponse.FailedImports.Count > 0 && containerClient != null)
                {
                    WriteFailedDocsToBlob("FailedImportDocs", containerClient, bulkImportResponse);
                }

                if (bulkImportResponse.BadInputDocuments.Count > 0 && containerClient != null)
                {
                    WriteFailedDocsToBlob("BadInputDocs", containerClient, bulkImportResponse);
                }

                LogMetrics(context, bulkImportResponse);
            }
            catch (Exception e)
            {
                Program.telemetryClient.TrackException(e);
            }

            Program.telemetryClient.Flush();
        }

        private static void WriteFailedDocsToBlob(string failureType, BlobContainerClient containerClient, BulkImportResponse bulkImportResponse)
        {
            string failedDocs;
            byte[] byteArray;
            BlobClient blobClient = containerClient.GetBlobClient(failureType + Guid.NewGuid().ToString() + ".csv");
            if (failureType == "FailedImportDocs")
            {
                failedDocs = JsonConvert.SerializeObject(String.Join(",", bulkImportResponse.FailedImports.First().DocumentsFailedToImport));
                byteArray = Encoding.ASCII.GetBytes(bulkImportResponse.FailedImports.First().BulkImportFailureException.GetType() + "|" + bulkImportResponse.FailedImports.First().DocumentsFailedToImport.Count + "|" + bulkImportResponse.FailedImports.First().BulkImportFailureException.Message.Substring(0, 100) + "|" + failedDocs);
            }
            else
            {
                failedDocs = JsonConvert.SerializeObject(String.Join(",", bulkImportResponse.BadInputDocuments));
                byteArray = Encoding.ASCII.GetBytes(failureType + ", " + bulkImportResponse.BadInputDocuments.Count + "|" + failedDocs);
            }
            using (var ms = new MemoryStream(byteArray))
            {
                blobClient.UploadAsync(ms, overwrite: true);
            }
        }

        private static void LogMetrics(IChangeFeedObserverContext context, BulkImportResponse bulkImportResponse)
        {
            Program.telemetryClient.TrackMetric("TotalInserted", bulkImportResponse.NumberOfDocumentsImported);
            Program.telemetryClient.TrackMetric("InsertedDocuments-Process:"
                + Process.GetCurrentProcess().Id, bulkImportResponse.NumberOfDocumentsImported);
            Program.telemetryClient.TrackMetric("TotalRUs", bulkImportResponse.TotalRequestUnitsConsumed);

            if (bulkImportResponse.BadInputDocuments.Count > 0)
            {
                Program.telemetryClient.TrackMetric("BadInputDocsCount", bulkImportResponse.BadInputDocuments.Count);
            }

            if (bulkImportResponse.FailedImports.Count > 0)
            {
                Program.telemetryClient.TrackMetric("FailedImportDocsCount", bulkImportResponse.FailedImports.First().DocumentsFailedToImport.Count);
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
                doc.SetPropertyValue(targetPartitionKey, isNestedAttribute == true ? GetNestedValue(doc, sourcePartitionKeys): doc.GetPropertyValue<string>(sourcePartitionKeys));      
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

        public static string GetNestedValue (Document doc, string path)
        {
            var jsonReader = JsonReaderWriterFactory.CreateJsonReader(Encoding.UTF8.GetBytes(doc.ToString()), new System.Xml.XmlDictionaryReaderQuotas());
            var root = XElement.Load(jsonReader);
            string value = root.XPathSelectElement("//"+path).Value;
            return value;
        }
    }
}
