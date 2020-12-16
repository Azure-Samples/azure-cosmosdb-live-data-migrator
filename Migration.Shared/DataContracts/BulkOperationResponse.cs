using System;
using System.Collections.Generic;

namespace Migration.Shared
{
    public class BulkOperationResponse<T>
    {
        public TimeSpan TotalTimeTaken { get; set; }
        public int SuccessfulDocuments { get; set; } = 0;
        public double TotalRequestUnitsConsumed { get; set; } = 0;
        public IReadOnlyList<T> FailedDocs { get; set; }
        public IReadOnlyList<string> DocFailures { get; set; }
        public IReadOnlyList<(T, Exception)> Failures { get; set; }
    }
}