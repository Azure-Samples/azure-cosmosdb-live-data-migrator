namespace MigrationConsoleApp
{
    using Azure.Storage.Blobs;
    using Microsoft.Azure.Documents.ChangeFeedProcessor;
    using Microsoft.Azure.Documents.Client;
    using IChangeFeedObserver = Microsoft.Azure.Documents.ChangeFeedProcessor.FeedProcessing.IChangeFeedObserver;
    using IChangeFeedObserverFactory = Microsoft.Azure.Documents.ChangeFeedProcessor.FeedProcessing.IChangeFeedObserverFactory;

    public class DocumentFeedObserverFactory: IChangeFeedObserverFactory
    {
        private DocumentClient destClient;
        private readonly string SourcePartitionKeys;
        private readonly string TargetPartitionKey;
        private DocumentCollectionInfo destCollInfo;
        private IDocumentTransformer documentTransformer;
        private BlobContainerClient containerClient;

        /// <summary>
        /// Initializes a new instance of the <see cref="DocumentFeedObserverFactory" /> class.
        /// Saves input DocumentClient and DocumentCollectionInfo parameters to class fields
        /// </summary>
        /// <param name="SourcePartitionKeys">Attributes from source collection to be mapped as PK in Target</param>
        /// <param name="TargetPartitionKey">PK attribute name in Target</param>
        /// <param name="destClient">Client connected to destination collection</param>
        /// <param name="destCollInfo">Destination collection information</param>
        /// <param name="docTransformer">Default Document Transformer</param>
        /// <param name="containerClient">Blob client to persist DLQ docs</param>
        public DocumentFeedObserverFactory(string SourcePartitionKeys, string TargetPartitionKey, DocumentClient destClient, DocumentCollectionInfo destCollInfo, IDocumentTransformer docTransformer, BlobContainerClient containerClient)
        {
            this.destCollInfo = destCollInfo;
            this.destClient = destClient;
            this.documentTransformer = docTransformer;
            this.containerClient = containerClient;
            this.SourcePartitionKeys = SourcePartitionKeys;
            this.TargetPartitionKey = TargetPartitionKey;
        }

        public IChangeFeedObserver CreateObserver()
        {
            DocumentFeedObserver newObserver = new DocumentFeedObserver(SourcePartitionKeys, TargetPartitionKey, this.destClient, this.destCollInfo, this.documentTransformer, this.containerClient);
            return newObserver;
        }
    }
}
