using Migration.Shared.DataContracts;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Migration.Executor.WebJob
{
    public interface IDocumentTransformer
    {
        Task<IEnumerable<DocumentMetadata>> TransformDocument(DocumentMetadata sourceDoc);
    }
}
