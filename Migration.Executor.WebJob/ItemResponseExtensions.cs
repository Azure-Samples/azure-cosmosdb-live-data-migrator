using Microsoft.Azure.Cosmos;
using Migration.Shared.DataContracts;
using System;
using System.Linq;
using System.Threading.Tasks;

namespace Migration.Executor.WebJob
{
    public static class ItemResponseExtensions
    {
        public static Task<OperationResponse<T>> CaptureOperationResponse<T>(
            this Task<ItemResponse<T>> task,
            T item)
        {
            return task.ContinueWith(itemResponse =>
            {
                if (itemResponse.IsCompleted && itemResponse.Exception == null)
                {
                    return new OperationResponse<T>()
                    {
                        Item = item,
                        IsSuccessful = true,
                        RequestUnitsConsumed = task.Result.RequestCharge
                    };
                }

                AggregateException innerExceptions = itemResponse.Exception.Flatten();
                if (innerExceptions
                    .InnerExceptions
                    .FirstOrDefault(innerEx => innerEx is CosmosException) is CosmosException cosmosException)
                {
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
