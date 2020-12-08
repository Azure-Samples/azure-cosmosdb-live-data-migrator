using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using Microsoft.ApplicationInsights;
using Microsoft.ApplicationInsights.DataContracts;
using Microsoft.ApplicationInsights.Extensibility;
using Migration.Shared.DataContracts;

namespace Migration.Shared
{
    public class TelemetryHelper
    {
        private static TelemetryHelper singletonInstance;
        private readonly TelemetryClient client;
        private readonly Dictionary<string, string> defaultProperties;

        private TelemetryHelper(
            TelemetryClient client,
            string sourceName)
        {
            if (String.IsNullOrWhiteSpace(sourceName)) { throw new ArgumentNullException(nameof(sourceName)); }

            this.client = client ?? throw new ArgumentNullException(nameof(client));
            this.defaultProperties = new Dictionary<string, string>
            {
                { "Source", sourceName },
            };
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

        public static void Initilize(
            TelemetryConfiguration telemetryConfig,
            string sourceName)
        {
            singletonInstance = new TelemetryHelper(new TelemetryClient(telemetryConfig), sourceName);
        }

        public void LogInfo(string format, params object[] args)
        {
            string message = String.Format(CultureInfo.InvariantCulture, format, args);
            Console.WriteLine(message);
            this.client.TrackTrace(message, SeverityLevel.Information, this.defaultProperties);
        }

        public void LogVerbose(string format, params object[] args)
        {
            string message = String.Format(CultureInfo.InvariantCulture, format, args);
            Console.WriteLine(message);
            this.client.TrackTrace(message, SeverityLevel.Verbose, this.defaultProperties);
        }

        public void LogWarning(string format, params object[] args)
        {
            string message = String.Format(CultureInfo.InvariantCulture, format, args);
            Console.WriteLine(message);
            this.client.TrackTrace(message, SeverityLevel.Warning, this.defaultProperties);
            this.client.Flush();
        }

        public void LogError(string format, params object[] args)
        {
            string message = String.Format(CultureInfo.InvariantCulture, format, args);
            Console.WriteLine(message);
            this.client.TrackTrace(message, SeverityLevel.Error, this.defaultProperties);
            this.client.Flush();
        }

        public void LogMetrics(BulkOperationResponse<DocumentMetadata> bulkOperationResponse)
        {
            this.client.TrackMetric("TotalInserted", bulkOperationResponse.SuccessfulDocuments, this.defaultProperties);
            this.client.TrackMetric("InsertedDocuments-Process:" +
                Process.GetCurrentProcess().Id, bulkOperationResponse.SuccessfulDocuments, this.defaultProperties);
            this.client.TrackMetric("TotalRUs", bulkOperationResponse.TotalRequestUnitsConsumed, this.defaultProperties);

            if (bulkOperationResponse.Failures.Count > 0)
            {
                this.client.TrackMetric("FailedImportDocsCount", bulkOperationResponse.FailedDocs.Count, this.defaultProperties);
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
            long etaMs)
        {
            this.client.TrackMetric("SourceCollectionCount", sourceCollectionCount, this.defaultProperties);
            this.client.TrackMetric("DestinationCollectionCount", currentDestinationCollectionCount, this.defaultProperties);
            this.client.TrackMetric("CurrentPercentage", currentPercentage, this.defaultProperties);
            this.client.TrackMetric("CurrentInsertRate", currentRate, this.defaultProperties);
            this.client.TrackMetric("AverageInsertRate", averageRate, this.defaultProperties);
            this.client.TrackMetric("ETA_ms", etaMs, this.defaultProperties);

            this.LogInfo(
                "CurrentPercentage = {0}, ETA_ms = {1}, Current rate = {2}, Average rate = {3}, Source count = {4}, Destination count = {5}",
                currentPercentage,
                etaMs,
                currentRate,
                averageRate,
                sourceCollectionCount,
                currentDestinationCollectionCount);

            this.client.Flush();
        }
    }
}