namespace MigrationConsoleApp
{
    using System;
    using System.Collections.Generic;
    using System.Configuration;
    using System.Diagnostics;
    using System.Threading.Tasks;
    using Microsoft.Azure.Documents;
    using Microsoft.Azure.Documents.ChangeFeedProcessor;
    using Microsoft.Azure.Documents.ChangeFeedProcessor.PartitionManagement;
    using Microsoft.Azure.Documents.Client;
    using Azure.Storage.Blobs;
    using Azure.Storage.Blobs.Specialized;
    using Microsoft.Azure.Documents.SystemFunctions;

    public class ChangeFeedProcessorHost
    {
        private MigrationConfig config;
        private IChangeFeedProcessor changeFeedProcessor;
        private DocumentClient destinationCollectionClient;

        public DocumentClient GetDestinationCollectionClient()
        {
            if (destinationCollectionClient == null)
            {
                destinationCollectionClient = new DocumentClient(
                    new Uri(config.DestUri), config.DestSecretKey,
                new ConnectionPolicy() { ConnectionMode = ConnectionMode.Direct, ConnectionProtocol = Protocol.Tcp },
                ConsistencyLevel.Eventual);
            }

            return destinationCollectionClient;
        }

        public ChangeFeedProcessorHost(MigrationConfig config)
        {
            this.config = config;
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
            using (DocumentClient client = new DocumentClient(new Uri(endPointUri), secretKey))
            {
                await client.CreateDatabaseIfNotExistsAsync(new Database { Id = databaseName });

                await client.CreateDocumentCollectionIfNotExistsAsync(
                    UriFactory.CreateDatabaseUri(databaseName),
                    new DocumentCollection() { Id = collectionName },
                    new RequestOptions { OfferThroughput = throughput });
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

        public async Task<IChangeFeedProcessor> RunChangeFeedHostAsync()
        {
            string hostName = Guid.NewGuid().ToString();
            Trace.TraceInformation("Host name {0}", hostName);

            // monitored collection info 
            var documentCollectionLocation = new DocumentCollectionInfo
            {
                Uri = new Uri(this.config.MonitoredUri),
                MasterKey = this.config.MonitoredSecretKey,
                DatabaseName = this.config.MonitoredDbName,
                CollectionName = this.config.MonitoredCollectionName
            };

            var policy = new ConnectionPolicy()
            {
                ConnectionMode = ConnectionMode.Direct,
                ConnectionProtocol = Protocol.Tcp
            };

            policy.PreferredLocations.Add("North Europe");

            // lease collection info 
            var leaseCollectionLocation = new DocumentCollectionInfo
            {
                Uri = new Uri(this.config.LeaseUri),
                MasterKey = this.config.LeaseSecretKey,
                DatabaseName = this.config.LeaseDbName,
                CollectionName = this.config.LeaseCollectionName,
                ConnectionPolicy = policy
            };

            // destination collection info 
            var destCollInfo = new DocumentCollectionInfo
            {
                Uri = new Uri(this.config.DestUri),
                MasterKey = this.config.DestSecretKey,
                DatabaseName = this.config.DestDbName,
                CollectionName = this.config.DestCollectionName
            };

            var processorOptions = new ChangeFeedProcessorOptions();
            if(config.DataAgeInHours.HasValue) {
                if (config.DataAgeInHours.Value >= 0)
                {
                    processorOptions.StartTime = DateTime.UtcNow.Subtract(TimeSpan.FromHours(config.DataAgeInHours.Value));
                }
            } else
            {
                processorOptions.StartFromBeginning = true;
            }

            processorOptions.LeaseRenewInterval = TimeSpan.FromSeconds(30);

            Trace.TraceInformation("Processor options Starts from Beginning - {0}, Lease renew interval - {1}",
                processorOptions.StartFromBeginning,
                processorOptions.LeaseRenewInterval.ToString());

            processorOptions.MaxItemCount = 1000;
            var destClient = new DocumentClient(destCollInfo.Uri, destCollInfo.MasterKey,
                new ConnectionPolicy() { ConnectionMode = ConnectionMode.Direct, ConnectionProtocol = Protocol.Tcp },
                ConsistencyLevel.Eventual);

            var docTransformer = new DefaultDocumentTransformer();

            //BlobServiceClient blobServiceClient = new BlobServiceClient("DefaultEndpointsProtocol=https;AccountName=revin;AccountKey=rmN8Esbnyal8q0keILZWx2XdXZpmTHXOVs0lNIigs/nhK25J25zWHWPxDik7LZ2mqIEolJclHPgyEtBOfa4NfA==;EndpointSuffix=core.windows.net");
            //BlobContainerClient containerClient = blobServiceClient.GetBlobContainerClient("cosmosdb-live-etl");

            BlobContainerClient containerClient = null;

            if (!String.IsNullOrEmpty(config.BlobConnectionString))
            {

                Console.WriteLine("blobConnectionString: " + this.config.BlobConnectionString);
                Console.WriteLine("blobContainerName: " + this.config.BlobContainerName);

                BlobServiceClient blobServiceClient = new BlobServiceClient(config.BlobConnectionString);
                containerClient = blobServiceClient.GetBlobContainerClient(config.BlobContainerName);

                await containerClient.CreateIfNotExistsAsync();
            } 
            

            //AppendBlobClient appendBlobClient = new AppendBlobClient("DefaultEndpointsProtocol=https;AccountName=revin;AccountKey=rmN8Esbnyal8q0keILZWx2XdXZpmTHXOVs0lNIigs/nhK25J25zWHWPxDik7LZ2mqIEolJclHPgyEtBOfa4NfA==;EndpointSuffix=core.windows.net", "cosmosdb-live-etl", "FailedImportDocs.csv");
            //appendBlobClient.AppendBlockAsync("hello");

            var docObserverFactory = new DocumentFeedObserverFactory(destClient, destCollInfo, docTransformer, containerClient);

            changeFeedProcessor = await new ChangeFeedProcessorBuilder()
                .WithObserverFactory(docObserverFactory)
                .WithHostName(hostName)
                .WithFeedCollection(documentCollectionLocation)
                .WithLeaseCollection(leaseCollectionLocation)
                .WithProcessorOptions(processorOptions)
                .WithFeedDocumentClient(new DocumentClient(documentCollectionLocation.Uri, documentCollectionLocation.MasterKey, policy, ConsistencyLevel.Eventual))
                .BuildAsync();
            await changeFeedProcessor.StartAsync().ConfigureAwait(false);
            return changeFeedProcessor;
        }
    }
}
