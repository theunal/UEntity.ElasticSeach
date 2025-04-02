using Microsoft.Extensions.DependencyInjection;
using Nest;
using System.Diagnostics.CodeAnalysis;

namespace UEntity.ElasticSeach;

public interface IEntityRepositoryElasticSeach<T> where T : class
{
    Task<T?> GetAsync(string id);
    Task AddAsync(T document, string id);
    Task AddRangeAsync<T, TKey>(IEnumerable<T> items, Func<T, TKey> keySelector, int chunkSize = 2000);
    Task UpdateAsync(T document, string id);
    Task DeleteAsync(string id);
}

public class EntityRepositoryElasticSeach<T>(string indexName) : IEntityRepositoryElasticSeach<T> where T : class
{
    public async Task<T?> GetAsync(string id)
    {
        var response = await UEntityElasticSearchExtensions.UEntityElasticClient!.GetAsync<T>(id, g => g.Index(indexName));
        return !response.IsValid || !response.Found ? null : response.Source;
    }
    public Task AddAsync(T document, string id)
    {
        return UEntityElasticSearchExtensions.UEntityElasticClient!.IndexAsync(document, idx => idx.Index(indexName).Id(id));
    }
    public async Task AddRangeAsync<T, TKey>(IEnumerable<T> items, Func<T, TKey> keySelector, int chunkSize = 2000)
    {
        var chunks = items.Chunk(chunkSize);
        foreach (var chunk in chunks)
        {
            var bulkDescriptor = new BulkDescriptor();
            foreach (var item in chunk)
            {
                var id = keySelector(item)?.ToString();
                if (string.IsNullOrEmpty(id)) continue;
                bulkDescriptor.Index<object>(op => op.Index(indexName).Id(id).Document(item!));
            }
            await UEntityElasticSearchExtensions.UEntityElasticClient!.BulkAsync(bulkDescriptor);
        }
    }
    public Task UpdateAsync(T document, string id)
    {
        return UEntityElasticSearchExtensions.UEntityElasticClient!.UpdateAsync<T>(id, u => u.Index(indexName).Doc(document));
    }
    public Task DeleteAsync(string id)
    {
        return UEntityElasticSearchExtensions.UEntityElasticClient!.DeleteAsync(new DeleteRequest(indexName, id));
    }
}
public static class UEntityElasticSearchExtensions
{
    public static ElasticClient? UEntityElasticClient;
    public static IServiceCollection AddUEntityElasticSeach([NotNull] this IServiceCollection services, ElasticClient client)
    {
        ArgumentNullException.ThrowIfNull(services);
        UEntityElasticClient = client;
        DbMonitor();
        return services;
    }
    private static async Task DbMonitor()
    {
        while (true)
        {
            try
            {
                var pingResponse = UEntityElasticClient!.Ping();
                if (!pingResponse.ApiCall.Success) throw new Exception();
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
                    UEntityElasticClient = new ElasticClient(UEntityElasticClient!.ConnectionSettings);

                    var pingResponse = UEntityElasticClient!.Ping();
                    if (pingResponse.ApiCall.Success)
                    {
                        // Bağlantı başarılıysa mesaj yazdır
                        Console.ForegroundColor = ConsoleColor.Green;
                        Console.WriteLine($"{DateTime.Now.ToString("u")} The Elasticsearch connection was successfully re-established.");
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