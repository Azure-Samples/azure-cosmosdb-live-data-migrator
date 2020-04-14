namespace MigrationConsoleApp
{
    using System;
    using System.Configuration;
    using System.Diagnostics;
    using System.Threading.Tasks;
    using Microsoft.Azure.Documents;
    using Microsoft.Azure.Documents.ChangeFeedProcessor;
    using Microsoft.Azure.Documents.ChangeFeedProcessor.PartitionManagement;
    using Microsoft.Azure.Documents.Client;

    public class ChangeFeedProcessorHost
    {
        private MigrationConfig config;
        public async Task StartAsync(MigrationConfig config)
        {
            this.config = config;

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
                "Starting destination (sink) collection creation: Url {0} - key {1} - dbName {2} - collectionName {3}",
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
                    new DocumentCollection(){Id = collectionName},
                    new RequestOptions { OfferThroughput = throughput});
            }
        }
        public async Task<IChangeFeedProcessor> RunChangeFeedHostAsync()
        {
            string hostName = Guid.NewGuid().ToString();
            Trace.TraceInformation("Host name {0}", hostName);

            // monitored collection info 
            DocumentCollectionInfo documentCollectionLocation = new DocumentCollectionInfo
            {
                Uri = new Uri(this.config.MonitoredUri),
                MasterKey = this.config.MonitoredSecretKey,
                DatabaseName = this.config.MonitoredDbName,
                CollectionName = this.config.MonitoredCollectionName
            };

            // lease collection info 
            DocumentCollectionInfo leaseCollectionLocation = new DocumentCollectionInfo
            {
                Uri = new Uri(this.config.LeaseUri),
                MasterKey = this.config.LeaseSecretKey,
                DatabaseName = this.config.LeaseDbName,
                CollectionName = this.config.LeaseCollectionName
            };

            // destination collection info 
            DocumentCollectionInfo destCollInfo = new DocumentCollectionInfo
            {
                Uri = new Uri(this.config.DestUri),
                MasterKey = this.config.DestSecretKey,
                DatabaseName = this.config.DestDbName,
                CollectionName = this.config.DestCollectionName
            };

            ChangeFeedProcessorOptions processorOptions = new ChangeFeedProcessorOptions();
            processorOptions.StartFromBeginning = true;

            processorOptions.LeaseRenewInterval = TimeSpan.FromSeconds(15);

            Trace.TraceInformation("Processor options Starts from Beginning - {0}, Lease renew interval - {1}",
                processorOptions.StartFromBeginning,
                processorOptions.LeaseRenewInterval.ToString());

            DocumentClient destClient = new DocumentClient(destCollInfo.Uri, destCollInfo.MasterKey);
            DocumentFeedObserverFactory docObserverFactory = new DocumentFeedObserverFactory(destClient, destCollInfo);
            var processor = await new ChangeFeedProcessorBuilder()
                .WithObserverFactory(docObserverFactory)
                .WithHostName(hostName)
                .WithFeedCollection(documentCollectionLocation)
                .WithLeaseCollection(leaseCollectionLocation)
                .WithProcessorOptions(processorOptions)
                .BuildAsync();
            await processor.StartAsync().ConfigureAwait(false);
            return processor;

        }
    }
}
