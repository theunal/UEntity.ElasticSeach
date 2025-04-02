# UEntity.ElasticSeach NuGet Package

## Introduction
The `UEntity.ElasticSeach` NuGet package provides a flexible and efficient repository pattern implementation for ElasticSeach. This package simplifies common ElasticSeach operations such as querying, inserting, updating, deleting, and pagination.

## Getting Started

### Installation
Install the package via NuGet:
```bash
Install-Package UEntity.ElasticSeach
```

### Configuration
To use the package, configure it in your `Program.cs` file by calling the `AddUEntityElasticSeach` extension method.

```csharp
var elasticsearchUri = "http://localhost:9200";
var settings = new ConnectionSettings(new Uri(elasticsearchUri))
    .DefaultIndex("your_default_index");

var client = new ElasticClient(settings);

var serviceCollection = new ServiceCollection();
serviceCollection.AddUEntityElasticSeach(client);
```

## Usage

```csharp
public class YourEntity
{
    public string Id { get; set; }
    public string Name { get; set; }
}

var repository = new EntityRepositoryElasticSeach<YourEntity>("your_index_name");

// Adding a single document
await repository.AddAsync(new YourEntity { Id = "1", Name = "Document 1" }, "1");

// Getting a document by ID
var document = await repository.GetAsync("1");

// Updating a document
await repository.UpdateAsync(new YourEntity { Id = "1", Name = "Updated Document" }, "1");

// Deleting a document
await repository.DeleteAsync("1");

// Adding a range of documents
var items = new List<YourEntity>
{
    new YourEntity { Id = "2", Name = "Document 2" },
    new YourEntity { Id = "3", Name = "Document 3" }
};
await repository.AddRangeAsync(items, item => item.Id);
```