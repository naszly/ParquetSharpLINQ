using ParquetSharpLINQ.Attributes;

namespace ParquetSharpLINQ.Benchmarks;

/// <summary>
/// Test entity matching generated SalesRecord schema, with indexed CustomerId.
/// </summary>
public class IndexedSalesRecord
{
    [ParquetColumn("id")] public long Id { get; set; }

    [ParquetColumn("product_name")] public string ProductName { get; set; } = string.Empty;

    [ParquetColumn("quantity")] public int Quantity { get; set; }

    [ParquetColumn("unit_price")] public decimal UnitPrice { get; set; }

    [ParquetColumn("total_amount")] public decimal TotalAmount { get; set; }

    [ParquetColumn("sale_date")] public DateTime SaleDate { get; set; }

    [ParquetColumn("customer_id")] public long CustomerId { get; set; }

    [ParquetColumn("client_id", Indexed = true)] public string ClientId { get; set; } = string.Empty;

    [ParquetColumn("is_discounted")] public bool IsDiscounted { get; set; }

    [ParquetColumn("year", IsPartition = true)]
    public int Year { get; set; }

    [ParquetColumn("month", IsPartition = true)]
    public int Month { get; set; }

    [ParquetColumn("region", IsPartition = true)]
    public string Region { get; set; } = string.Empty;
}
