using System;
using System.Globalization;
using System.IO;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;

namespace Migration.Shared.DataContracts
{
    /// <summary>
    ///  Represents an abstract resource type in the Azure Cosmos DB service.
    ///  All Azure Cosmos DB resources, such as <see cref="Database"/>, <see cref="DocumentCollection"/>, and <see cref="Document"/> extend this abstract type.
    /// </summary>
    public abstract class Resource : JsonSerializable
    {
        internal static DateTime UnixStartTime = new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc);

        /// <summary>
        /// Initializes a new instance of the <see cref="Resource"/> class for the Azure Cosmos DB service.
        /// </summary>
        protected Resource()
        {
        }

        /// <summary>
        /// Copy constructor for a <see cref="Resource"/> used in the Azure Cosmos DB service.
        /// </summary>
        protected Resource(Resource resource)
        {
            this.Id = resource.Id;
            this.ResourceId = resource.ResourceId;
            this.SelfLink = resource.SelfLink;
            this.AltLink = resource.AltLink;
            this.Timestamp = resource.Timestamp;
            this.ETag = resource.ETag;
        }

        /// <summary>
        /// Gets or sets the Id of the resource in the Azure Cosmos DB service.
        /// </summary>
        /// <value>The Id associated with the resource.</value>
        /// <remarks>
        /// <para>
        /// Every resource within an Azure Cosmos DB database account needs to have a unique identifier.
        /// Unlike <see cref="Resource.ResourceId"/>, which is set internally, this Id is settable by the user and is not immutable.
        /// </para>
        /// <para>
        /// When working with document resources, they too have this settable Id property.
        /// If an Id is not supplied by the user the SDK will automatically generate a new GUID and assign its value to this property before
        /// persisting the document in the database.
        /// You can override this auto Id generation by setting the disableAutomaticIdGeneration parameter on the <see cref="Microsoft.Azure.Documents.Client.DocumentClient"/> instance to true.
        /// This will prevent the SDK from generating new Ids.
        /// </para>
        /// <para>
        /// The following characters are restricted and cannot be used in the Id property:
        ///  '/', '\\', '?', '#'
        /// </para>
        /// </remarks>
        [JsonProperty(PropertyName = "id")]
        public virtual string Id
        {
            get => this.GetValue<string>("id");
            set => this.SetValue("id", value);
        }

        /// <summary>
        /// Gets or sets the Resource Id associated with the resource in the Azure Cosmos DB service.
        /// </summary>
        /// <value>
        /// The Resource Id associated with the resource.
        /// </value>
        /// <remarks>
        /// A Resource Id is the unique, immutable, identifier assigned to each Azure Cosmos DB
        /// resource whether that is a database, a collection or a document.
        /// These resource ids are used when building up SelfLinks, a static addressable Uri for each resource within a database account.
        /// </remarks>
        [JsonProperty(PropertyName = "_rid")]
        public virtual string ResourceId
        {
            get => this.GetValue<string>("_rid");
            set => this.SetValue("_rid", value);
        }

        /// <summary>
        /// Gets the self-link associated with the resource from the Azure Cosmos DB service.
        /// </summary>
        /// <value>The self-link associated with the resource.</value>
        /// <remarks>
        /// A self-link is a static addressable Uri for each resource within a database account and follows the Azure Cosmos DB resource model.
        /// E.g. a self-link for a document could be dbs/db_resourceid/colls/coll_resourceid/documents/doc_resourceid
        /// </remarks>
        [JsonProperty(PropertyName = "_self")]
        public string SelfLink
        {
            get => this.GetValue<string>("_self");
            set => this.SetValue("_self", value);
        }

        /// <summary>
        /// Gets the alt-link associated with the resource from the Azure Cosmos DB service.
        /// </summary>
        /// <value>The alt-link associated with the resource.</value>
        [JsonIgnore]
        public string AltLink
        {
            get;
            set;
        }

        /// <summary>
        /// Gets the last modified timestamp associated with the resource from the Azure Cosmos DB service.
        /// </summary>
        /// <value>The last modified timestamp associated with the resource.</value>
        [JsonProperty(PropertyName = "_ts")]
        [JsonConverter(typeof(UnixDateTimeConverter))]
        public virtual DateTime Timestamp
        {
            get =>
                // Add seconds to the unix start time
                UnixStartTime.AddSeconds(this.GetValue<double>("_ts"));
            set => this.SetValue("_ts", (ulong)(value - UnixStartTime).TotalSeconds);
        }

        /// <summary>
        /// Gets the entity tag associated with the resource from the Azure Cosmos DB service.
        /// </summary>
        /// <value>
        /// The entity tag associated with the resource.
        /// </value>
        /// <remarks>
        /// ETags are used for concurrency checking when updating resources.
        /// </remarks>
        [JsonProperty(PropertyName = "_etag")]
        public string ETag
        {
            get => this.GetValue<string>("_etag");
            set => this.SetValue("_etag", value);
        }

        /// <summary>
        /// Sets property value associated with the specified property name in the Azure Cosmos DB service.
        /// </summary>
        /// <param name="propertyName">The name of the property.</param>
        /// <param name="propertyValue">The property value.</param>
        public void SetPropertyValue(string propertyName, object propertyValue)
        {
            base.SetValue(propertyName, propertyValue);
        }

        /// <summary>
        /// Gets property value associated with the specified property name from the Azure Cosmos DB service.
        /// </summary>
        /// <typeparam name="T">The type of the property.</typeparam>
        /// <param name="propertyName">The name of the property.</param>
        /// <returns>The property value.</returns>
        public T GetPropertyValue<T>(string propertyName)
        {
            return base.GetValue<T>(propertyName);
        }

        /// <summary>
        /// Validates the property, by calling it, in case of any errors exception is thrown
        /// </summary>
        internal override void Validate()
        {
            base.Validate();
            this.GetValue<string>("id");
            this.GetValue<string>("_rid");
            this.GetValue<string>("_self");
            this.GetValue<double>("_ts");
            this.GetValue<string>("_etag");
        }

        /// <summary>
        /// Serialize to a byte array via SaveTo for the Azure Cosmos DB service.
        /// </summary>
        public byte[] ToByteArray()
        {
            using (MemoryStream ms = new MemoryStream())
            {
                this.SaveTo(ms);
                return ms.ToArray();
            }
        }

        private sealed class UnixDateTimeConverter : DateTimeConverterBase
        {
            private static readonly DateTime UnixStartTime = new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc);

            /// <summary>
            /// Writes the JSON representation of the DateTime object.
            /// </summary>
            /// <param name="writer">The Newtonsoft.Json.JsonWriter to write to.</param>
            /// <param name="value">The value.</param>
            /// <param name="serializer">The calling serializer.</param>
            public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer)
            {
                if (value is DateTime time)
                {
                    Int64 totalSeconds = (Int64)(time - UnixStartTime).TotalSeconds;
                    writer.WriteValue(totalSeconds);
                }
                else
                {
                    throw new ArgumentException("Expecting DateTime value.", "value");
                }
            }

            /// <summary>
            /// Reads the JSON representation of the DateTime object.
            /// </summary>
            /// <param name="reader">The Newtonsoft.Json.JsonReader to read from.</param>
            /// <param name="objectType">Type of the object.</param>
            /// <param name="existingValue">The existing value of object being read.</param>
            /// <param name="serializer">The calling serializer.</param>
            /// <returns>
            /// The DateTime object value.
            /// </returns>
            public override object ReadJson(JsonReader reader, Type objectType, object existingValue, JsonSerializer serializer)
            {
                if (reader.TokenType != Newtonsoft.Json.JsonToken.Integer)
                {
                    throw new Exception("Expecting reader to read Integer");
                }

                double totalSeconds;

                try
                {
                    totalSeconds = Convert.ToDouble(reader.Value, CultureInfo.InvariantCulture);
                }
                catch
                {
                    throw new Exception("Expecting reader value to be compatible with double conversion.");
                }

                return UnixStartTime.AddSeconds(totalSeconds);
            }
        }
    }
}