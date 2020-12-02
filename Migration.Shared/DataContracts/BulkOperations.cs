using System.Collections.Generic;
using System.Diagnostics;
using System.Threading.Tasks;
using System.Linq;

namespace Migration.Shared.DataContracts
{
    public class BulkOperations<T>
    {
        public readonly List<Task<OperationResponse<T>>> Tasks;
        private readonly Stopwatch stopwatch = Stopwatch.StartNew();
        public BulkOperations(int operationCount)
        {
            this.Tasks = new List<Task<OperationResponse<T>>>(operationCount);
        }
        public async Task<BulkOperationResponse<T>> ExecuteAsync()
        {
            await Task.WhenAll(this.Tasks);
            this.stopwatch.Stop();
            return new BulkOperationResponse<T>()
            {
                TotalTimeTaken = this.stopwatch.Elapsed,
                TotalRequestUnitsConsumed = this.Tasks.Sum(task => task.Result.RequestUnitsConsumed),
                SuccessfulDocuments = this.Tasks.Count(task => task.Result.IsSuccessful),
                FailedDocs = this.Tasks
                    .Where(task => !task.Result.IsSuccessful)
                    .Select(task => task.Result.Item)
                    .ToList(),
                DocFailures = this.Tasks
                    .Where(task => !task.Result.IsSuccessful)
                    .Select(task => task.Result.CosmosException.Message)
                    .ToList(),
                Failures = this.Tasks
                    .Where(task => !task.Result.IsSuccessful)
                    .Select(task => (task.Result.Item, task.Result.CosmosException))
                    .ToList()
            };
        }
    }
}
