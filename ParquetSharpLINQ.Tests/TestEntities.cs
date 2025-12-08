using ParquetSharpLINQ.Attributes;

namespace ParquetSharpLINQ.Tests;

/// <summary>
/// Test entity with various column types - source generation enabled by default
/// </summary>
public class TestEntity
{
    [ParquetColumn("id")] public long Id { get; set; }

    [ParquetColumn("name")] public string? Name { get; set; }

    [ParquetColumn("amount")] public decimal Amount { get; set; }

    [ParquetColumn("count")] public int Count { get; set; }

    [ParquetColumn("is_active")] public bool IsActive { get; set; }

    [ParquetColumn("created_date")] public DateTime CreatedDate { get; set; }

    [ParquetColumn("year", IsPartition = true)]
    public int Year { get; set; }

    [ParquetColumn("region", IsPartition = true)]
    public string? Region { get; set; }
}

/// <summary>
/// Test entity with source generation - for testing generator
/// </summary>
public class GeneratedTestEntity
{
    [ParquetColumn("id")] public long Id { get; set; }

    [ParquetColumn("name")] public string? Name { get; set; }

    [ParquetColumn("amount")] public decimal Amount { get; set; }
}

/// <summary>
/// Test entity with required fields
/// </summary>
public class TestEntityWithRequired
{
    [ParquetColumn("id", ThrowOnMissingOrNull = true)]
    public long Id { get; set; }

    [ParquetColumn("name", ThrowOnMissingOrNull = true)]
    public string Name { get; set; } = string.Empty;

    [ParquetColumn("optional")] public string? Optional { get; set; }
}

/// <summary>
/// Test entity with nullable fields
/// </summary>
public class TestEntityWithNullables
{
    [ParquetColumn("id")] public long Id { get; set; }

    [ParquetColumn("nullable_int")] public int? NullableInt { get; set; }

    [ParquetColumn("nullable_decimal")] public decimal? NullableDecimal { get; set; }

    [ParquetColumn("nullable_date")] public DateTime? NullableDate { get; set; }
}

/// <summary>
/// Test entity without any attributes - for negative testing
/// </summary>
public class EntityWithoutAttributes
{
    public long Id { get; set; }
    public string? Name { get; set; }
}

/// <summary>
/// Test entity with custom column names
/// </summary>
public class TestEntityWithCustomNames
{
    [ParquetColumn("db_id")] public long Id { get; set; }

    [ParquetColumn("full_name")] public string? Name { get; set; }

    [ParquetColumn] public decimal Amount { get; set; }
}

/// <summary>
/// Test entity with DateTime fields - for testing DateTime support
/// </summary>
public class TestEntityWithDateTime
{
    [ParquetColumn("id")] public long Id { get; set; }

    [ParquetColumn("created_date")] public DateTime CreatedDate { get; set; }

    [ParquetColumn("updated_date")] public DateTime? UpdatedDate { get; set; }

    [ParquetColumn("deleted_date")] public DateTime? DeletedDate { get; set; }
}

/// <summary>
/// Test entity with DateTime partition - for testing DateTime partition support
/// </summary>
public class TestEntityWithDateTimePartition
{
    [ParquetColumn("id")] public long Id { get; set; }

    [ParquetColumn("name")] public string? Name { get; set; }

    [ParquetColumn("event_date", IsPartition = true)]
    public DateTime EventDate { get; set; }

    [ParquetColumn("region", IsPartition = true)]
    public string? Region { get; set; }
}

/// <summary>
/// Test entity with DateOnly partition - for testing DateOnly partition support
/// </summary>
public class TestEntityWithDateOnlyPartition
{
    [ParquetColumn("id")] public long Id { get; set; }

    [ParquetColumn("name")] public string? Name { get; set; }

    [ParquetColumn("data_day", IsPartition = true)]
    public DateOnly DataDay { get; set; }

    [ParquetColumn("region", IsPartition = true)]
    public string? Region { get; set; }
}