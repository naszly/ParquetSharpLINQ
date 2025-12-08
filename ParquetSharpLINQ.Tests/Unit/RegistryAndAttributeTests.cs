using ParquetSharpLINQ.Attributes;

namespace ParquetSharpLINQ.Tests.Unit;

[TestFixture]
[Category("Unit")]
public class ParquetColumnAttributeTests
{
    [Test]
    public void DefaultConstructor_SetsDefaultValues()
    {
        var attr = new ParquetColumnAttribute();

        Assert.That(attr.Name, Is.Null);
        Assert.That(attr.IsPartition, Is.False);
        Assert.That(attr.ThrowOnMissingOrNull, Is.False);
    }

    [Test]
    public void ConstructorWithName_SetsName()
    {
        var attr = new ParquetColumnAttribute("column_name");

        Assert.That(attr.Name, Is.EqualTo("column_name"));
    }

    [Test]
    public void Properties_CanBeSet()
    {
        var attr = new ParquetColumnAttribute
        {
            Name = "test",
            IsPartition = true,
            ThrowOnMissingOrNull = true
        };

        Assert.That(attr.Name, Is.EqualTo("test"));
        Assert.That(attr.IsPartition, Is.True);
        Assert.That(attr.ThrowOnMissingOrNull, Is.True);
    }
}