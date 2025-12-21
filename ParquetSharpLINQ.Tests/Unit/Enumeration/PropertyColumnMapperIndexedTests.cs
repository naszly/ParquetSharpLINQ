using ParquetSharpLINQ.Enumeration;

namespace ParquetSharpLINQ.Tests.Unit.Enumeration;

[TestFixture]
[Category("Unit")]
[Category("Enumeration")]
public class PropertyColumnMapperIndexedTests
{
    [Test]
    public void GetIndexedColumnNames_ReturnsOnlyIndexedColumns()
    {
        var indexed = PropertyColumnMapper<IndexedEntity>.GetIndexedColumnNames();

        Assert.That(indexed, Does.Contain("client_id"));
        Assert.That(indexed, Does.Not.Contain("name"));
        Assert.That(indexed, Does.Not.Contain("region"));
    }

    [Test]
    public void IsIndexedColumnName_UsesColumnNameMapping()
    {
        Assert.That(PropertyColumnMapper<IndexedEntity>.IsIndexedColumnName("client_id"), Is.True);
        Assert.That(PropertyColumnMapper<IndexedEntity>.IsIndexedColumnName("CLIENT_ID"), Is.True);
        Assert.That(PropertyColumnMapper<IndexedEntity>.IsIndexedColumnName("name"), Is.False);
    }

}
