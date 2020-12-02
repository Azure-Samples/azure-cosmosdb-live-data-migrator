using Microsoft.Azure.Documents;
using Migration.Shared.DataContracts;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

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
