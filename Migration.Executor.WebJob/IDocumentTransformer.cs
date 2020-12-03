using System.Collections.Generic;
using System.Threading.Tasks;
using Migration.Shared.DataContracts;

namespace Migration.Executor.WebJob
{
    public interface IDocumentTransformer
    {
        Task<IEnumerable<DocumentMetadata>> TransformDocument(DocumentMetadata sourceDoc);
    }
}