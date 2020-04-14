using Microsoft.Azure.Documents;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MigrationConsoleApp
{
    public interface IDocumentTransformer
    {
        Task<IEnumerable<Document>> TransformDocument(Document sourceDoc);
    }
}
