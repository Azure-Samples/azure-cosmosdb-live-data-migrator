using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure.Documents;
using Newtonsoft.Json;

namespace MigrationConsoleApp
{
    public class DocumentMultiplierTransformer : IDocumentTransformer
    {
        public Task<IEnumerable<Document>> TransformDocument(Document sourceDoc)
        {
            var docs = new List<Document>();

            byte[] byteArray = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(sourceDoc));
            sourceDoc.Id = Guid.NewGuid().ToString();

            docs.Add(sourceDoc);
            return Task.FromResult(docs.AsEnumerable());
        }
    }
}
