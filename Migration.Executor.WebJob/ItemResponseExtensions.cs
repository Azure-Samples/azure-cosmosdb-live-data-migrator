using System;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Microsoft.Azure.Cosmos;
using Migration.Shared.DataContracts;

namespace Migration.Executor.WebJob
{
    public static class ItemResponseExtensions
    {
        public static Task<OperationResponse<T>> CaptureOperationResponse<T>(
            this Task<ResponseMessage> task,
            T item,
            Boolean ignoreConflicts)
        {
            return task.ContinueWith(itemResponse =>
            {
                if (itemResponse.IsCompleted && itemResponse.Exception == null)
                {
                    return new OperationResponse<T>()
                    {
                        Item = item,
                        IsSuccessful = true,
                        RequestUnitsConsumed = task.Result.Headers.RequestCharge
                    };
                }

                AggregateException innerExceptions = itemResponse.Exception.Flatten();
                if (innerExceptions
                    .InnerExceptions
                    .FirstOrDefault(innerEx => innerEx is CosmosException) is CosmosException cosmosException)
                {
                    if (ignoreConflicts && cosmosException.StatusCode == HttpStatusCode.Conflict)
                    {
                        return new OperationResponse<T>()
                        {
                            Item = item,
                            IsSuccessful = true,
                            RequestUnitsConsumed = task?.Result?.Headers.RequestCharge ?? 0
                        };
                    }

                    return new OperationResponse<T>()
                    {
                        Item = item,
                        RequestUnitsConsumed = cosmosException.RequestCharge,
                        IsSuccessful = false,
                        CosmosException = cosmosException
                    };
                }

                return new OperationResponse<T>()
                {
                    Item = item,
                    IsSuccessful = false,
                    CosmosException = innerExceptions.InnerExceptions.FirstOrDefault()
                };
            });
        }
    }
}