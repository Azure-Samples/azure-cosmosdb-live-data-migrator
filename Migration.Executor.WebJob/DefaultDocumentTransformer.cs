using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Migration.Shared.DataContracts;

namespace Migration.Executor.WebJob
{
    public class DefaultDocumentTransformer : IDocumentTransformer
    {
        public Task<IEnumerable<DocumentMetadata>> TransformDocument(DocumentMetadata sourceDoc)
        {
            List<DocumentMetadata> docs = new List<DocumentMetadata>
            {
                sourceDoc
            };

            return Task.FromResult(docs.AsEnumerable());
        }
    }
}