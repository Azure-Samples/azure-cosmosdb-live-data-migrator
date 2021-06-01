using System;
using System.Text.Json;
using Microsoft.Azure.Cosmos;

namespace Migration.Shared.DataContracts
{
    public class DocumentMetadata
    {
        private readonly JsonElement jsonRoot;

        /// <summary>
        /// Initializes a new instance of the <see cref="Document"/> class for the Azure Cosmos DB service.
        /// </summary>
        public DocumentMetadata(JsonElement jsonRoot, String rawJson)
        {
            if (String.IsNullOrWhiteSpace(rawJson)) { throw new ArgumentNullException(nameof(rawJson)); }
            this.jsonRoot = jsonRoot;
            this.RawJson = rawJson;
        }

        public String RawJson { get; }

        public PartitionKey PK { get; set; }

        /// <summary>
        /// Gets property value associated with the specified property name from the Azure Cosmos DB service.
        /// </summary>
        /// <typeparam name="T">The type of the property.</typeparam>
        /// <param name="propertyName">The name of the property.</param>
        /// <returns>The property value.</returns>
        public string GetPropertyValue(string propertyName)
        {
            return this.jsonRoot.GetProperty(propertyName).GetString();
        }
    }
}