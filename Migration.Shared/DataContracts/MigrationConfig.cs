using System;
using Newtonsoft.Json;

namespace Migration.Shared.DataContracts
{
    public class MigrationConfig
    {
        [JsonProperty("sourcePartitionKeys")]
        public string SourcePartitionKeys { get; set; }

        [JsonProperty("targetPartitionKey")]
        public string TargetPartitionKey { get; set; }

        [JsonProperty("monitoredAccount")]
        public string MonitoredAccount { get; set; }

        [JsonProperty("monitoredDbName")]
        public string MonitoredDbName { get; set; }

        [JsonProperty("monitoredCollectionName")]
        public string MonitoredCollectionName { get; set; }

        [JsonProperty("monitoredThroughput")]
        public int MonitoredThroughput { get; set; }

        [JsonProperty("leaseAccount")]
        public string LeaseAccount { get; set; }

        [JsonProperty("leaseDbName")]
        public string LeaseDbName { get; set; }

        [JsonProperty("leaseCollectionName")]
        public string LeaseCollectionName { get; set; }

        [JsonProperty("leaseThroughput")]
        public int LeaseThroughput { get; set; }

        [JsonProperty("destAccount")]
        public string DestAccount { get; set; }

        [JsonProperty("destDbName")]
        public string DestDbName { get; set; }

        [JsonProperty("destCollectionName")]
        public string DestCollectionName { get; set; }

        [JsonProperty("destThroughput")]
        public int DestThroughput { get; set; }

        [JsonProperty("dataAgeInHours")]
        public double? DataAgeInHours { get; set; }

        [JsonProperty("completed")]
        public bool Completed { get; set; }

        [JsonProperty("id")]
        public string Id { get; set; }

        [JsonProperty("startTime")]
        public long StartTimeEpochMs { get; set; }

        [JsonProperty("deadLetterStorageAccountName")]
        public string DeadLetterStorageAccountName { get; set; }

        [JsonProperty("deadLetterContainerName")]
        public string DeadLetterContainerName { get; set; }

        [JsonProperty("statistics.count")]
        public long MigratedDocumentCount { get; set; }

        [JsonProperty("statistics.expectedDurationLeft")]
        public long ExpectedDurationLeft { get; set; }

        [JsonProperty("statistics.avgRate")]
        public double AvgRate { get; set; }

        [JsonProperty("statistics.currentRate")]
        public double CurrentRate { get; set; }

        [JsonProperty("statistics.sourceCount")]
        public long SourceCountSnapshot { get; set; }

        [JsonProperty("statistics.destinationCount")]
        public long DestinationCountSnapshot { get; set; }

        [JsonProperty("statistics.percentageCompleted")]
        public double PercentageCompleted { get; set; }

        [JsonProperty("statistics.lastUpdated")]
        public long StatisticsLastUpdatedEpochMs { get; set; }

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
            get; set;
        }

        [JsonIgnore]
        public string SourceIdentifier => string.Concat(this.MonitoredAccount, "/", this.MonitoredDbName, "/", this.MonitoredCollectionName);

        [JsonIgnore]
        public string DestinationIdentifier => string.Concat(this.DestAccount, "/", this.DestDbName, "/", this.DestCollectionName);

        [JsonIgnore]
        public string EstimatedTimeToComplete => DateTimeOffset.UtcNow.AddMilliseconds(this.ExpectedDurationLeft).ToString("u");

        [JsonIgnore]
        public string StartTime => DateTimeOffset.FromUnixTimeMilliseconds(this.StartTimeEpochMs).ToString("u");

        [JsonIgnore]
        public string StatisticsLastUpdated => this.StatisticsLastUpdatedEpochMs > 0 ?
            DateTimeOffset.FromUnixTimeMilliseconds(this.StatisticsLastUpdatedEpochMs).ToString("u"):
            "";
    }

}
