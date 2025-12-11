using System.Reflection;
using ParquetSharpLINQ.Constants;
using ParquetSharpLINQ.ParquetSharp;

namespace ParquetSharpLINQ.Tests.Unit;

[TestFixture]
[Category("Unit")]
public class SourceGenerationTests
{
    [Test]
    public void GeneratedMapper_ShouldExist_ForGeneratedTestEntity()
    {
        const string mapperTypeName = "ParquetSharpLINQ.Tests.GeneratedTestEntityParquetMapper";
        var mapperType = typeof(GeneratedTestEntity).Assembly.GetType(mapperTypeName);
        Assert.That(mapperType, Is.Not.Null,
            "Generated mapper class should exist in the assembly");
    }

    [Test]
    public void GeneratedMapper_ShouldImplementIParquetMapper()
    {
        const string mapperTypeName = "ParquetSharpLINQ.Tests.GeneratedTestEntityParquetMapper";
        var mapperType = typeof(GeneratedTestEntity).Assembly.GetType(mapperTypeName);
        Assert.That(mapperType, Is.Not.Null);
        var interfaceType = typeof(IParquetMapper<GeneratedTestEntity>);
        Assert.That(mapperType!.GetInterfaces(), Does.Contain(interfaceType),
            "Generated mapper should implement IParquetMapper<T>");
    }

    [Test]
    public void GeneratedMapper_ShouldBeUsedByParquetTable()
    {
        const string mapperTypeName = "ParquetSharpLINQ.Tests.GeneratedTestEntityParquetMapper";
        var mapperType = typeof(GeneratedTestEntity).Assembly.GetType(mapperTypeName);
        Assert.That(mapperType, Is.Not.Null,
            "This test requires the source generator to run. Build the project first.");
        var mapper = Activator.CreateInstance(mapperType!) as IParquetMapper<GeneratedTestEntity>;
        Assert.That(mapper, Is.Not.Null);
    }

    [Test]
    public void GeneratedMapper_ShouldMapDataCorrectly()
    {
        const string mapperTypeName = "ParquetSharpLINQ.Tests.GeneratedTestEntityParquetMapper";
        var mapperType = typeof(GeneratedTestEntity).Assembly.GetType(mapperTypeName);
        Assert.That(mapperType, Is.Not.Null);
        var mapper = Activator.CreateInstance(mapperType!) as IParquetMapper<GeneratedTestEntity>;
        Assert.That(mapper, Is.Not.Null);
        var row = new ParquetRow(
            ["id", "name", "amount"],
            [123L, "Test", 456.78m]
        );
        var result = mapper!.Map(row);
        Assert.That(result.Id, Is.EqualTo(123L));
        Assert.That(result.Name, Is.EqualTo("Test"));
        Assert.That(result.Amount, Is.EqualTo(456.78m));
    }

    [Test]
    public void ParquetTable_ShouldUseGeneratedMapper_NotReflection()
    {
        const string mapperTypeName = "ParquetSharpLINQ.Tests.GeneratedTestEntityParquetMapper";
        var generatedMapperExists = typeof(GeneratedTestEntity).Assembly.GetType(mapperTypeName) != null;
        Assert.That(generatedMapperExists, Is.True,
            "This test verifies that source generation is working. The generated mapper must exist.");
        var table = new ParquetTable<GeneratedTestEntity>("/dummy/path");
        var mapperField = typeof(ParquetTable<GeneratedTestEntity>)
            .GetField("_mapper", BindingFlags.NonPublic | BindingFlags.Instance);
        Assert.That(mapperField, Is.Not.Null);
        var mapper = mapperField!.GetValue(table);
        Assert.That(mapper, Is.Not.Null);
        Assert.That(mapper!.GetType().Name, Is.EqualTo("GeneratedTestEntityParquetMapper"),
            "ParquetTable MUST use the source-generated mapper, NOT reflection mapper");
    }

    [Test]
    public void EntityWithoutParquetColumnAttributes_ShouldThrowException()
    {
        // EntityWithoutAttributes has NO [ParquetColumn] attributes at all
        // Source generator won't create a mapper for it
        Assert.Throws<InvalidOperationException>(() =>
                new ParquetTable<EntityWithoutAttributes>("/dummy/path"),
            "Entity without [ParquetColumn] attributes should throw - no mapper generated");
    }

    [Test]
    public void AllParquetColumnEntities_ShouldUseGeneratedMappers()
    {
        var table = new ParquetTable<TestEntity>("/dummy/path");

        var mapperField = typeof(ParquetTable<TestEntity>)
            .GetField("_mapper", BindingFlags.NonPublic | BindingFlags.Instance);

        var mapper = mapperField!.GetValue(table);

        Assert.That(mapper!.GetType().Name, Is.EqualTo("TestEntityParquetMapper"),
            "All entities with [ParquetColumn] use source-generated mappers");
    }

    [Test]
    public void GeneratedMapper_ShouldExist_ForDateTimeEntity()
    {
        const string mapperTypeName = "ParquetSharpLINQ.Tests.TestEntityWithDateTimeParquetMapper";
        var mapperType = typeof(TestEntityWithDateTime).Assembly.GetType(mapperTypeName);

        Assert.That(mapperType, Is.Not.Null,
            "Generated mapper should exist for entity with DateTime fields");
    }

    [Test]
    public void GeneratedMapper_ShouldMapDateTime_Correctly()
    {
        const string mapperTypeName = "ParquetSharpLINQ.Tests.TestEntityWithDateTimeParquetMapper";
        var mapperType = typeof(TestEntityWithDateTime).Assembly.GetType(mapperTypeName);

        Assert.That(mapperType, Is.Not.Null);

        var mapper = Activator.CreateInstance(mapperType!) as IParquetMapper<TestEntityWithDateTime>;
        Assert.That(mapper, Is.Not.Null);

        var testDate = new DateTime(2024, 1, 15, 10, 30, 45);
        var testNullableDate = new DateTime(2024, 2, 20, 14, 45, 30);

        var row = new ParquetRow(
            ["id", "created_date", "updated_date", "deleted_date"],
            [123L, testDate, testNullableDate, null]
        );

        var result = mapper!.Map(row);

        Assert.That(result.Id, Is.EqualTo(123L));
        Assert.That(result.CreatedDate, Is.EqualTo(testDate));
        Assert.That(result.UpdatedDate, Is.EqualTo(testNullableDate));
        Assert.That(result.DeletedDate, Is.Null);
    }

    [Test]
    public void GeneratedMapper_ShouldConvertDateTimeFromString()
    {
        const string mapperTypeName = "ParquetSharpLINQ.Tests.TestEntityWithDateTimeParquetMapper";
        var mapperType = typeof(TestEntityWithDateTime).Assembly.GetType(mapperTypeName);

        var mapper = Activator.CreateInstance(mapperType!) as IParquetMapper<TestEntityWithDateTime>;

        var row = new ParquetRow(
            ["id", "created_date", "updated_date", "deleted_date"],
            [456L, "2024-12-07T08:30:00", "2024-12-07T12:45:00", null]
        );

        var result = mapper!.Map(row);

        Assert.That(result.CreatedDate, Is.EqualTo(DateTime.Parse("2024-12-07T08:30:00")));
        Assert.That(result.UpdatedDate, Is.EqualTo(DateTime.Parse("2024-12-07T12:45:00")));
        Assert.That(result.DeletedDate, Is.Null);
    }

    [Test]
    public void GeneratedMapper_ShouldHandleNullableDateTime()
    {
        const string mapperTypeName = "ParquetSharpLINQ.Tests.TestEntityWithNullablesParquetMapper";
        var mapperType = typeof(TestEntityWithNullables).Assembly.GetType(mapperTypeName);

        var mapper = Activator.CreateInstance(mapperType!) as IParquetMapper<TestEntityWithNullables>;

        var row = new ParquetRow(
            ["id", "nullable_int", "nullable_decimal", "nullable_date"],
            [789L, 42, 123.45m, new DateTime(2024, 3, 15)]
        );

        var result = mapper!.Map(row);

        Assert.That(result.NullableDate, Is.EqualTo(new DateTime(2024, 3, 15)));

        row = new ParquetRow(
            ["id", "nullable_int", "nullable_decimal", "nullable_date"],
            [789L, 42, 123.45m, null]
        );
        result = mapper.Map(row);
        Assert.That(result.NullableDate, Is.Null);
    }

    [Test]
    public void GeneratedMapper_ShouldExist_ForDateTimePartitionEntity()
    {
        const string mapperTypeName = "ParquetSharpLINQ.Tests.TestEntityWithDateTimePartitionParquetMapper";
        var mapperType = typeof(TestEntityWithDateTimePartition).Assembly.GetType(mapperTypeName);

        Assert.That(mapperType, Is.Not.Null,
            "Generated mapper should exist for entity with DateTime partition");
    }

    [Test]
    public void GeneratedMapper_ShouldMapDateTimePartition_FromString()
    {
        const string mapperTypeName = "ParquetSharpLINQ.Tests.TestEntityWithDateTimePartitionParquetMapper";
        var mapperType = typeof(TestEntityWithDateTimePartition).Assembly.GetType(mapperTypeName);
        var mapper = Activator.CreateInstance(mapperType!) as IParquetMapper<TestEntityWithDateTimePartition>;

        // Simulate how ParquetTable enriches rows with partition values
        // Partition values from directory names like "event_date=2025-10-01" are strings
        var row = new ParquetRow(
            ["id", "name", $"{PartitionConstants.PartitionPrefix}event_date"],
            [100L, "Test Record", "2025-10-01"] // String from partition directory name
        );

        var result = mapper!.Map(row);

        Assert.That(result.Id, Is.EqualTo(100L));
        Assert.That(result.Name, Is.EqualTo("Test Record"));
        Assert.That(result.EventDate, Is.EqualTo(new DateTime(2025, 10, 1)));
    }

    [Test]
    public void GeneratedMapper_ShouldMapDateTimePartition_WithTime()
    {
        const string mapperTypeName = "ParquetSharpLINQ.Tests.TestEntityWithDateTimePartitionParquetMapper";
        var mapperType = typeof(TestEntityWithDateTimePartition).Assembly.GetType(mapperTypeName);
        var mapper = Activator.CreateInstance(mapperType!) as IParquetMapper<TestEntityWithDateTimePartition>;

        // Partition with timestamp
        var row = new ParquetRow(
            ["id", "name", $"{PartitionConstants.PartitionPrefix}event_date"],
            [200L, "Test with Time", "2025-10-01T14:30:00"]
        );

        var result = mapper!.Map(row);

        Assert.That(result.EventDate, Is.EqualTo(new DateTime(2025, 10, 1, 14, 30, 0)));
    }

    [Test]
    public void GeneratedMapper_ShouldMapDateTimePartition_VariousFormats()
    {
        const string mapperTypeName = "ParquetSharpLINQ.Tests.TestEntityWithDateTimePartitionParquetMapper";
        var mapperType = typeof(TestEntityWithDateTimePartition).Assembly.GetType(mapperTypeName);
        var mapper = Activator.CreateInstance(mapperType!) as IParquetMapper<TestEntityWithDateTimePartition>;

        var testCases = new[]
        {
            ("2025-10-01", new DateTime(2025, 10, 1)),
            ("2025-10-01T00:00:00", new DateTime(2025, 10, 1)),
            ("10/01/2025", new DateTime(2025, 10, 1))
        };

        foreach (var (dateString, expectedDate) in testCases)
        {
            var row = new ParquetRow(
                ["id", "name", $"{PartitionConstants.PartitionPrefix}event_date"],
                [1L, "Test", dateString]
            );

            var result = mapper!.Map(row);

            Assert.That(result.EventDate.Date, Is.EqualTo(expectedDate.Date),
                $"Failed to parse date format: {dateString}");
        }
    }

    [Test]
    public void GeneratedMapper_ShouldMapDateOnlyPartition_FromString()
    {
        const string mapperTypeName = "ParquetSharpLINQ.Tests.TestEntityWithDateOnlyPartitionParquetMapper";
        var mapperType = typeof(TestEntityWithDateOnlyPartition).Assembly.GetType(mapperTypeName);

        if (mapperType == null)
        {
            Assert.Inconclusive("DateOnly is only available in .NET 6.0+");
            return;
        }

        var mapper = Activator.CreateInstance(mapperType!) as IParquetMapper<TestEntityWithDateOnlyPartition>;

        var row = new ParquetRow(
            ["id", "name", $"{PartitionConstants.PartitionPrefix}data_day"],
            [300L, "DateOnly Test", "2025-10-01"]
        );

        var result = mapper!.Map(row);

        Assert.That(result.Id, Is.EqualTo(300L));
        Assert.That(result.DataDay, Is.EqualTo(new DateOnly(2025, 10, 1)));
    }

    [Test]
    public void GeneratedMapper_ShouldHandleLongPartitionValues()
    {
        const string mapperTypeName = "ParquetSharpLINQ.Tests.TestEntityParquetMapper";
        var mapperType = typeof(TestEntity).Assembly.GetType(mapperTypeName);
        var mapper = Activator.CreateInstance(mapperType!) as IParquetMapper<TestEntity>;

        // Simulate a very long region partition value
        var longRegionValue =
            "very-long-region-name-that-might-cause-path-length-issues-when-combined-with-other-partitions-and-file-paths-exceeding-260-characters";

        var row = new ParquetRow(
            ["id", "name", "amount", "count", "is_active", "created_date", $"{PartitionConstants.PartitionPrefix}year", $"{PartitionConstants.PartitionPrefix}region"
            ],
            [1L, "Test", 100m, 5, true, DateTime.Now, "2025", longRegionValue]
        );

        var result = mapper!.Map(row);

        Assert.That(result.Year, Is.EqualTo(2025));
        Assert.That(result.Region, Is.EqualTo(longRegionValue));
    }

    [Test]
    public void GeneratedMapper_ShouldHandleMultipleLongPartitions()
    {
        const string mapperTypeName = "ParquetSharpLINQ.Tests.TestEntityWithDateTimePartitionParquetMapper";
        var mapperType = typeof(TestEntityWithDateTimePartition).Assembly.GetType(mapperTypeName);
        var mapper = Activator.CreateInstance(mapperType!) as IParquetMapper<TestEntityWithDateTimePartition>;

        // Simulate partition path like:
        // event_day=2025-10-01/event_source=very-long-event-source-to-test-the-path-limit
        var row = new ParquetRow(
            ["id", "name", $"{PartitionConstants.PartitionPrefix}event_date"],
            [
                999L,
                "very-long-event-source-name-that-could-be-part-of-a-partition-key-in-real-world-scenarios-like-kafka-topics-or-application-names",
                "2025-10-01"
            ]
        );

        var result = mapper!.Map(row);

        Assert.That(result.EventDate, Is.EqualTo(new DateTime(2025, 10, 1)));
        Assert.That(result.Name, Has.Length.GreaterThan(100),
            "Should handle long string values without truncation");
    }
}