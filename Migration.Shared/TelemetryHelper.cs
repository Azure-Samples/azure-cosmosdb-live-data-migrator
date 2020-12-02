using Microsoft.ApplicationInsights;
using Microsoft.ApplicationInsights.DataContracts;
using Microsoft.ApplicationInsights.Extensibility;
using Microsoft.Azure.Documents;
using Migration.Shared.DataContracts;
using System;
using System.Diagnostics;
using System.Globalization;

namespace Migration.Shared
{
    public class TelemetryHelper
    {
        private static TelemetryHelper singletonInstance;
        private readonly TelemetryClient client;

        private TelemetryHelper(TelemetryClient client)
        {
            this.client = client ?? throw new ArgumentNullException(nameof(client));
        }

        public static TelemetryHelper Singleton
        {
            get
            {
                if (singletonInstance == null)
                {
                    throw new InvalidOperationException("TelemetryHelper has not yet been initialized.");
                }

                return singletonInstance;
            }
        }

        public static void Initilize(TelemetryConfiguration telemetryConfig)
        {
            singletonInstance = new TelemetryHelper(new TelemetryClient(telemetryConfig));
        }

        public void LogInfo(string format, params object[] args)
        {
            string message = String.Format(CultureInfo.InvariantCulture, format, args);
            Console.WriteLine(message);
            this.client.TrackTrace(message, SeverityLevel.Information);
        }

        public void LogVerbose(string format, params object[] args)
        {
            string message = String.Format(CultureInfo.InvariantCulture, format, args);
            Console.WriteLine(message);
            this.client.TrackTrace(message, SeverityLevel.Verbose);
        }

        public void LogWarning(string format, params object[] args)
        {
            string message = String.Format(CultureInfo.InvariantCulture, format, args);
            Console.WriteLine(message);
            this.client.TrackTrace(message, SeverityLevel.Warning);
        }

        public void LogError(string format, params object[] args)
        {
            string message = String.Format(CultureInfo.InvariantCulture, format, args);
            Console.WriteLine(message);
            this.client.TrackTrace(message, SeverityLevel.Error);
        }

        public void LogMetrics(BulkOperationResponse<DocumentMetadata> bulkOperationResponse)
        {
            this.client.TrackMetric("TotalInserted", bulkOperationResponse.SuccessfulDocuments);
            this.client.TrackMetric("InsertedDocuments-Process:" +
                Process.GetCurrentProcess().Id, bulkOperationResponse.SuccessfulDocuments);
            this.client.TrackMetric("TotalRUs", bulkOperationResponse.TotalRequestUnitsConsumed);

            if (bulkOperationResponse.Failures.Count > 0)
            {
                this.client.TrackMetric("FailedImportDocsCount", bulkOperationResponse.FailedDocs.Count);
            }

            this.LogInfo(
                "TotalInserted = {0}, InsertedDocuments-Process {1} = {2}, TotalRUs = {3}, FailedImportDocsCount = {4}",
                bulkOperationResponse.SuccessfulDocuments,
                Process.GetCurrentProcess().Id,
                bulkOperationResponse.SuccessfulDocuments,
                bulkOperationResponse.TotalRequestUnitsConsumed,
                bulkOperationResponse.FailedDocs.Count);

            this.client.Flush();
        }

        public void TrackStatistics(
            long sourceCollectionCount,
            long currentDestinationCollectionCount,
            double currentPercentage,
            double currentRate,
            double averageRate,
            double eta)
        {
            this.client.TrackMetric("SourceCollectionCount", sourceCollectionCount);
            this.client.TrackMetric("DestinationCollectionCount", currentDestinationCollectionCount);
            this.client.TrackMetric("CurrentPercentage", currentPercentage);
            this.client.TrackMetric("CurrentInsertRate", currentRate);
            this.client.TrackMetric("AverageInsertRate", averageRate);
            this.client.TrackMetric("ETA", eta);

            this.LogInfo(
                "CurrentPercentage = {0}, ETA = {1}, Current rate = {2}, Average rate = {3}, Source count = {4}, Destination count = {5}",
                currentPercentage,
                eta,
                currentRate,
                averageRate,
                sourceCollectionCount,
                currentDestinationCollectionCount);

            this.client.Flush();
        }
    }
}
