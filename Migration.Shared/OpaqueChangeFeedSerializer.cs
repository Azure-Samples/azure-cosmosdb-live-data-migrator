using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Text.Json;
using Migration.Shared.DataContracts;

namespace Migration.Shared
{
    internal class OpaqueChangeFeedSerializer : DefaultSerializer
    {
        public OpaqueChangeFeedSerializer(): base()
        {
        }

        public override T FromStream<T>(Stream stream)
        {
            if (typeof(T) != typeof(DocumentMetadata) &&
                typeof(T) != typeof(DocumentMetadata[]))
            {
                return base.FromStream<T>(stream);
            }

            string json;
            using (stream)
            {
                using (StreamReader sr = new StreamReader(stream))
                {
                    json = sr.ReadToEnd();
                }
            }

            JsonDocumentOptions options = new JsonDocumentOptions
            {
                AllowTrailingCommas = true,
                CommentHandling = JsonCommentHandling.Skip,
                MaxDepth = 9999
            };


            if (typeof(T) == typeof(DocumentMetadata))
            {
                JsonDocument jsonDoc = JsonDocument.Parse(json, options);

                return (T)(object)new DocumentMetadata(jsonDoc.RootElement, json);
            }
            else
            {
                List<DocumentMetadata> list = new List<DocumentMetadata>();

                JsonDocument rootDoc = JsonDocument.Parse(json, options);

                foreach (JsonElement element in rootDoc.RootElement.EnumerateArray())
                {
                    list.Add(new DocumentMetadata(element, element.GetRawText()));
                }

                return (T)(object)list.ToArray();
            }
        }

        public override Stream ToStream<T>(T input)
        {
            if (typeof(T) != typeof(DocumentMetadata))
            {
                return base.ToStream(input);
            }

            MemoryStream ms = new MemoryStream(UTF8Encoding.UTF8.GetBytes(((DocumentMetadata)(object)input).RawJson));

            return ms;
        }
    }
}
