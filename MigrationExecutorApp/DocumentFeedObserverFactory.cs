namespace MigrationConsoleApp
{
    using Azure.Storage.Blobs;
    using Microsoft.Azure.Documents.ChangeFeedProcessor;
    using Microsoft.Azure.Documents.Client;
    using IChangeFeedObserver = Microsoft.Azure.Documents.ChangeFeedProcessor.FeedProcessing.IChangeFeedObserver;
    using IChangeFeedObserverFactory = Microsoft.Azure.Documents.ChangeFeedProcessor.FeedProcessing.IChangeFeedObserverFactory;
    using Azure.Storage.Blobs.Specialized;

    public class DocumentFeedObserverFactory: IChangeFeedObserverFactory
    {
        private DocumentClient destClient;
        private DocumentCollectionInfo destCollInfo;
        private IDocumentTransformer documentTransformer;
        //private AppendBlobClient appendBlobClient;
        private BlobContainerClient containerClient;

        /// <summary>
        /// Initializes a new instance of the <see cref="DocumentFeedObserverFactory" /> class.
        /// Saves input DocumentClient and DocumentCollectionInfo parameters to class fields
        /// </summary>
        /// <param name="destClient">Client connected to destination collection</param>
        /// <param name="destCollInfo">Destination collection information</param>
        /// /// <param name="docTransformer">Destination collection information</param>
        public DocumentFeedObserverFactory(DocumentClient destClient, DocumentCollectionInfo destCollInfo, IDocumentTransformer docTransformer, BlobContainerClient containerClient)
        {
            this.destCollInfo = destCollInfo;
            this.destClient = destClient;
            this.documentTransformer = docTransformer;
            this.containerClient = containerClient;
        }

        public IChangeFeedObserver CreateObserver()
        {
            DocumentFeedObserver newObserver = new DocumentFeedObserver(this.destClient, this.destCollInfo, this.documentTransformer, this.containerClient);
            return newObserver;
        }
    }
}
