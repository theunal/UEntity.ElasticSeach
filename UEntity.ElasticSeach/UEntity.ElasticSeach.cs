using Elasticsearch.Net;
using Microsoft.Extensions.DependencyInjection;
using Nest;
using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;

namespace UEntity.ElasticSeach;

public interface IEntityRepositoryElasticSeach<T> where T : class
{
    Task<T?> GetAsync(string id);
    Task<IndexResponse> AddAsync(T document, string id);
    Task AddRangeAsync(Dictionary<string, T> entities, int chunkSize = 10 * 1000);
    Task<UpdateResponse<T>> UpdateAsync(T document, string id);
    Task<DeleteResponse> DeleteAsync(string id);
    Task<DeleteByQueryResponse> ExecuteDeleteAsync(Func<QueryContainerDescriptor<T>, QueryContainer> filter);
}

public class EntityRepositoryElasticSeach<T>(string indexName) : IEntityRepositoryElasticSeach<T> where T : class
{
    public async Task<T?> GetAsync(string id)
    {
        var response = await UEntityElasticSearchExtensions.UEntityElasticClient!.GetAsync<T>(id, g => g.Index(indexName.ToLower()));
        return !response.IsValid || !response.Found ? null : response.Source;
    }
    public Task<IndexResponse> AddAsync(T document, string id)
    {
        return UEntityElasticSearchExtensions.UEntityElasticClient!.IndexAsync(document, idx => idx.Index(indexName.ToLower()).Id(id));
    }
    public async Task AddRangeAsync(Dictionary<string, T> entities, int chunkSize = 10 * 1000)
    {
        foreach (var chunk in entities.Chunk(chunkSize))
        {
            var operations = new BulkOperationsCollection<IBulkOperation>();
            foreach (var kvp in chunk)
            {
                operations.Add(new BulkIndexOperation<T>(kvp.Value)
                {
                    Index = indexName.ToLower(),
                    Id = kvp.Key
                });
            }
            await UEntityElasticSearchExtensions.UEntityElasticClient!.BulkAsync(new BulkRequest
            {
                Operations = operations
            });
        }
    }
    public Task<UpdateResponse<T>> UpdateAsync(T document, string id)
    {
        return UEntityElasticSearchExtensions.UEntityElasticClient!.UpdateAsync<T>(id, u => u.Index(indexName.ToLower()).Doc(document));
    }
    public Task<DeleteResponse> DeleteAsync(string id)
    {
        return UEntityElasticSearchExtensions.UEntityElasticClient!.DeleteAsync(new DeleteRequest(indexName.ToLower(), id));
    }
    public Task<DeleteByQueryResponse> ExecuteDeleteAsync(Func<QueryContainerDescriptor<T>, QueryContainer> filter)
    {
        return UEntityElasticSearchExtensions.UEntityElasticClient!.DeleteByQueryAsync<T>(q => q.Index(indexName.ToLower()).Query(filter));
    }

    public BulkResponse Bulk(Dictionary<string, T> entities)
    {
        var operations = new BulkOperationsCollection<IBulkOperation>();
        foreach (var kvp in entities)
        {
            operations.Add(new BulkIndexOperation<T>(kvp.Value)
            {
                Index = indexName.ToLower(),
                Id = kvp.Key
            });
        }
        return UEntityElasticSearchExtensions.UEntityElasticClient!.Bulk(new BulkRequest
        {
            Operations = operations
        });
    }
    public Task<BulkResponse> BulkAsync(Dictionary<string, T> entities)
    {
        var operations = new BulkOperationsCollection<IBulkOperation>();
        foreach (var kvp in entities)
        {
            operations.Add(new BulkIndexOperation<T>(kvp.Value)
            {
                Index = indexName.ToLower(),
                Id = kvp.Key
            });
        }
        return UEntityElasticSearchExtensions.UEntityElasticClient!.BulkAsync(new BulkRequest
        {
            Operations = operations
        });
    }

    public CountResponse Count(Func<QueryContainerDescriptor<T>, QueryContainer>? querySelector = null)
    {
        return UEntityElasticSearchExtensions.UEntityElasticClient!.Count<T>(c => c.Index(indexName.ToLower()).Query(querySelector));
    }
    public Task<CountResponse> CountAsync(Func<QueryContainerDescriptor<T>, QueryContainer>? querySelector = null)
    {
        return UEntityElasticSearchExtensions.UEntityElasticClient!.CountAsync<T>(c => c.Index(indexName.ToLower()).Query(querySelector));
    }

    /// <summary>
    /// Retrieves data from the database using pagination.
    /// </summary>
    /// <param name="offset">Number of records to skip (e.g., page * size).</param>
    /// <param name="limit">Number of records to be retrieved (must be 100 or less).</param>
    /// <returns>Paginated data list.</returns>
    /// <remarks>
    /// <b>Note:</b> The value of `page * size` can be at most 10,000.
    /// </remarks>
    public PaginateElastic<T> GetListPaginate(
        int page,
        int size,
        Func<QueryContainerDescriptor<T>, QueryContainer>? filter = null,
        List<Func<(Expression<Func<T, object>> Expression, SortOrder Order)>>? sort = null)
    {
        page = page < 1 ? 1 : page;
        size = size <= 0 ? 5 : size;

        var from = (page - 1) * size;

        IList<ISort>? sortList = GetSortList(sort);

        var searchRequest = new SearchRequest<T>(indexName.ToLower())
        {
            From = from,
            Size = size,
            Query = filter != null ? filter(new QueryContainerDescriptor<T>()) : new MatchAllQuery(),
            Sort = sortList
        };

        var countTask = Count(filter);
        var itemsTask = UEntityElasticSearchExtensions.UEntityElasticClient!.Search<T>(searchRequest);

        long total_count = countTask.Count;
        var pages_count = (long)Math.Ceiling(total_count / (double)size);

        return new PaginateElastic<T>
        {
            Page = page,
            Size = size,
            TotalCount = total_count,
            PagesCount = pages_count,
            HasPrevious = page > 1,
            HasNext = page < pages_count,
            Items = itemsTask.Documents
        };
    }

    /// <summary>
    /// Retrieves data from the database using pagination.
    /// </summary>
    /// <param name="offset">Number of records to skip (e.g., page * size).</param>
    /// <param name="limit">Number of records to be retrieved (must be 100 or less).</param>
    /// <returns>Paginated data list.</returns>
    /// <remarks>
    /// <b>Note:</b> The value of `page * size` can be at most 10,000.
    /// </remarks>
    public async Task<PaginateElastic<T>> GetListPaginateAsync(
        int page,
        int size,
        Func<QueryContainerDescriptor<T>, QueryContainer>? filter = null,
        List<Func<(Expression<Func<T, object>> Expression, SortOrder Order)>>? sort = null,
        CancellationToken cancellationToken = default)
    {
        page = page < 1 ? 1 : page;
        size = size <= 0 ? 5 : size;

        var from = (page - 1) * size;

        IList<ISort>? sortList = GetSortList(sort);

        var searchRequest = new SearchRequest<T>(indexName.ToLower())
        {
            From = from,
            Size = size,
            Query = filter != null ? filter(new QueryContainerDescriptor<T>()) : new MatchAllQuery(),
            Sort = sortList
        };

        var countTask = CountAsync(filter);
        var itemsTask = UEntityElasticSearchExtensions.UEntityElasticClient!
            .SearchAsync<T>(searchRequest, cancellationToken);

        await Task.WhenAll(countTask, itemsTask);

        long total_count = countTask.Result.Count;
        var pages_count = (long)Math.Ceiling(total_count / (double)size);

        return new PaginateElastic<T>
        {
            Page = page,
            Size = size,
            TotalCount = total_count,
            PagesCount = pages_count,
            HasPrevious = page > 1,
            HasNext = page < pages_count,
            Items = itemsTask.Result.Documents
        };
    }
    private static IList<ISort>? GetSortList(List<Func<(Expression<Func<T, object>> Expression, SortOrder Order)>>? sort)
    {
        return sort?.Count > 0 ?
            [.. sort
            .Select(x =>
            {
                var (expression, order) = x();
                return new FieldSort
                {
                    Field = Infer.Field(expression),
                    Order = order
                };
            }).Cast<ISort>()] : null;
    }
}

public record PaginateElastic<T>
{
    public int Page { get; set; }
    public int Size { get; set; }
    public long TotalCount { get; set; }
    public long PagesCount { get; set; }
    public bool HasPrevious { get; set; }
    public bool HasNext { get; set; }
    public IReadOnlyCollection<T> Items { get; set; } = null!;
}

public static class UEntityElasticSearchExtensions
{
    public static ElasticClient? UEntityElasticClient;
    public static ConnectionSettings? ConnectionSettings;
    public static IServiceCollection AddUEntityElasticSeach([NotNull] this IServiceCollection services, ConnectionSettings settings)
    {
        ConnectionSettings = settings;
        UEntityElasticClient = new ElasticClient(settings);
        ArgumentNullException.ThrowIfNull(services);
        DbMonitor();
        return services;
    }
    private static async Task DbMonitor()
    {
        bool first = true;
        while (true)
        {
            try
            {
                var pingResponse = UEntityElasticClient!.Ping();

                if (!pingResponse.ApiCall.Success)
                    throw new Exception();
                else if (first)
                {
                    Console.ForegroundColor = ConsoleColor.Green;
                    Console.WriteLine($"{DateTime.Now.ToString("u")} Elasticsearch connection successful");
                    Console.ResetColor();
                }

                first = false;
            }
            catch (Exception e)
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine($"\n{DateTime.Now.ToString("u")} Elasticsearch connection failed: {e.Message}");

                try
                {
                    // Elasticsearch bağlantısını yeniden kurma denemesi
                    Console.ForegroundColor = ConsoleColor.Yellow;
                    Console.WriteLine($"{DateTime.Now.ToString("u")} Re-establishing the Elasticsearch connection...");

                    // Yeni bir ElasticClient ile yeniden bağlantı kuruluyor
                    UEntityElasticClient = new ElasticClient(ConnectionSettings);

                    var pingResponse = UEntityElasticClient!.Ping();
                    if (pingResponse.ApiCall.Success)
                    {
                        // Bağlantı başarılıysa mesaj yazdır
                        Console.ForegroundColor = ConsoleColor.Green;
                        Console.WriteLine($"{DateTime.Now.ToString("u")} Elasticsearch connection was successfully re-established.");
                    }
                }
                catch (Exception ex)
                {
                    Console.ForegroundColor = ConsoleColor.Red;
                    Console.WriteLine($"{DateTime.Now.ToString("u")} Elasticsearch reconnection failure: {ex.Message}");
                }

                Console.ResetColor();
            }

            // Her 10 saniyede bir kontrol yap
            await Task.Delay(TimeSpan.FromSeconds(10));
        }
    }
}