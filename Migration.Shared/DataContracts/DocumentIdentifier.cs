using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Migration.Shared.DataContracts
{
    public class DocumentIdentifier
    {
        internal DocumentIdentifier(string pk, string id, string etag)
        {
            if (String.IsNullOrWhiteSpace(etag)) { throw new ArgumentNullException(nameof(etag)); }

            this.PartitionKey = pk ?? throw new ArgumentNullException(nameof(pk));
            this.Id = id ?? throw new ArgumentNullException(nameof(id));
            this.Etag = etag;
        }

        public String PartitionKey { get; private set; }

        public String Id { get; private set; }

        public String Etag { get; private set; }

        public override string ToString()
        {
            return String.Format(
                CultureInfo.InvariantCulture,
                "PK={0}|ID={1}|ETAG={2}",
                this.PartitionKey,
                this.Id,
                this.Etag);
        }

        public static DocumentIdentifier FromString(String rawIdentifierString)
        {
            if (String.IsNullOrWhiteSpace(rawIdentifierString)) { throw new ArgumentNullException(nameof(rawIdentifierString)); }

            if (!rawIdentifierString.StartsWith("\"PK="))
            {
                throw new ArgumentException(
                    String.Format(
                        CultureInfo.InvariantCulture,
                        "Partition key missing in document identifier '{0}'.",
                        rawIdentifierString),
                    nameof(rawIdentifierString));
            }

            int indexIDPrefix = rawIdentifierString.IndexOf("|ID=");
            if (indexIDPrefix < 0)
            {
                throw new ArgumentException(
                    String.Format(
                    CultureInfo.InvariantCulture,
                    "ID missing in document identifier '{0}'.",
                    rawIdentifierString),
                    nameof(rawIdentifierString));
            }


            int indexEtagPrefix = rawIdentifierString.IndexOf("|ETAG=");
            if (indexEtagPrefix < 0)
            {
                throw new ArgumentException(
                    String.Format(
                    CultureInfo.InvariantCulture,
                    "Etag missing in document identifier '{0}'.",
                    rawIdentifierString),
                    nameof(rawIdentifierString));
            }

            return new DocumentIdentifier(
                rawIdentifierString[4..indexIDPrefix],
                rawIdentifierString[(indexIDPrefix + 4)..indexEtagPrefix],
                rawIdentifierString[(indexEtagPrefix + 6)..(rawIdentifierString.Length - 1)]);
        }
    }
}
