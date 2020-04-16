using Newtonsoft.Json;

namespace MigrationConsoleApp
{
    public class MigrationConfig
    {
        [JsonProperty("sourcePartitionKeys")]
        public string SourcePartitionKeys { get; set; }

        [JsonProperty("targetPartitionKey")]
        public string TargetPartitionKey { get; set; }

        [JsonProperty("monitoredUri")]
        public string MonitoredUri { get; set; }

        [JsonProperty("monitoredSecretKey")]
        public string MonitoredSecretKey { get; set; }

        [JsonProperty("monitoredDbName")]
        public string MonitoredDbName { get; set; }

        [JsonProperty("monitoredCollectionName")]
        public string MonitoredCollectionName { get; set; }

        [JsonProperty("monitoredThroughput")]
        public int MonitoredThroughput { get; set; }

        [JsonProperty("leaseUri")]
        public string LeaseUri { get; set; }

        [JsonProperty("leaseSecretKey")]
        public string LeaseSecretKey { get; set; }

        [JsonProperty("leaseDbName")]
        public string LeaseDbName { get; set; }

        [JsonProperty("leaseCollectionName")]
        public string LeaseCollectionName { get; set; }

        [JsonProperty("leaseThroughput")]
        public int LeaseThroughput { get; set; }

        [JsonProperty("destUri")]
        public string DestUri { get; set; }

        [JsonProperty("destSecretKey")]
        public string DestSecretKey { get; set; }

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
        public long StartTime { get; set; }

        [JsonProperty("blobConnectionString")]
        public string BlobConnectionString { get; set; }

        [JsonProperty("blobContainerName")]
        public string BlobContainerName { get; set; }
    }
}
