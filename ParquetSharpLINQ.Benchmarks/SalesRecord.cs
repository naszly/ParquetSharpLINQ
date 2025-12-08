using ParquetSharpLINQ.Attributes;

namespace ParquetSharpLINQ.Benchmarks;

/// <summary>
/// Test entity for performance benchmarks
/// </summary>
public class SalesRecord
{
    [ParquetColumn("id")] public long Id { get; set; }

    [ParquetColumn("product_name")] public string ProductName { get; set; } = string.Empty;

    [ParquetColumn("quantity")] public int Quantity { get; set; }

    [ParquetColumn("unit_price")] public decimal UnitPrice { get; set; }

    [ParquetColumn("total_amount")] public decimal TotalAmount { get; set; }

    [ParquetColumn("sale_date")] public DateTime SaleDate { get; set; }

    [ParquetColumn("customer_id")] public long CustomerId { get; set; }

    [ParquetColumn("is_discounted")] public bool IsDiscounted { get; set; }

    // Partition columns
    [ParquetColumn("year", IsPartition = true)]
    public int Year { get; set; }

    [ParquetColumn("month", IsPartition = true)]
    public int Month { get; set; }

    [ParquetColumn("region", IsPartition = true)]
    public string Region { get; set; } = string.Empty;
}