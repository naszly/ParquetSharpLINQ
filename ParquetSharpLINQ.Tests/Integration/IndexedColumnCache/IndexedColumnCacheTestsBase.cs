using ParquetSharpLINQ.Enumeration;

namespace ParquetSharpLINQ.Tests.Integration.IndexedColumnCache;

[TestFixture]
[Category("Integration")]
public abstract class IndexedColumnCacheTestsBase
{
    protected abstract ParquetTable<IndexedSalesRecord> CreateTable();
    protected abstract int GetIndexReadCount(string columnName);
    private const string ClientPrefixNext = "f";
    private const string ClientPrefixDefault = "0";

    private (int Count, int IndexReads) ExecuteQueryAndGetIndexReads(string columnName, Func<int> executeQuery)
    {
        var before = GetIndexReadCount(columnName);
        var count = executeQuery();
        var after = GetIndexReadCount(columnName);
        return (count, after - before);
    }

    [Test]
    public void Integration_IndexedColumnCache_IsBuiltOnceAcrossQueries()
    {
        using var table = CreateTable();

        Assert.That(
            PropertyColumnMapper<IndexedSalesRecord>.GetIndexedColumnNames(),
            Does.Contain("client_id"));

        var (firstQueryCount, firstIndexReads) = ExecuteQueryAndGetIndexReads(
            "client_id",
            () => table.Count(r => r.ClientId.StartsWith(ClientPrefixDefault)));

        var (secondQueryCount, secondIndexReads) = ExecuteQueryAndGetIndexReads(
            "client_id",
            () => table.Count(r => r.ClientId.StartsWith(ClientPrefixDefault)));

        Assert.That(firstQueryCount, Is.GreaterThanOrEqualTo(0));
        Assert.That(secondQueryCount, Is.GreaterThan(0));
        Assert.That(firstIndexReads, Is.GreaterThan(0));
        Assert.That(secondIndexReads, Is.EqualTo(0));
    }

    [Test]
    public void Integration_IndexedColumnCache_IsReusedAcrossQueries()
    {
        using var table = CreateTable();

        var (firstQueryCount, firstIndexReads) = ExecuteQueryAndGetIndexReads(
            "client_id",
            () => table.Count(r => r.ClientId.StartsWith(ClientPrefixDefault)));

        var (secondQueryCount, secondIndexReads) = ExecuteQueryAndGetIndexReads(
            "client_id",
            () => table.Count(r => r.ClientId.StartsWith(ClientPrefixNext)));

        Assert.That(firstQueryCount, Is.GreaterThanOrEqualTo(0));
        Assert.That(secondQueryCount, Is.GreaterThan(0));
        Assert.That(firstIndexReads, Is.GreaterThan(0));
        Assert.That(secondIndexReads, Is.EqualTo(0));
    }

}
