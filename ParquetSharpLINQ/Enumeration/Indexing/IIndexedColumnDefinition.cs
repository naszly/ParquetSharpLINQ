using System.Linq.Expressions;
using ParquetSharpLINQ.Interfaces;

namespace ParquetSharpLINQ.Enumeration.Indexing;

public interface IIndexedColumnDefinition
{
    string ColumnName { get; }

    Type PropertyType { get; }

    RowGroupIndex BuildRowGroupIndex(IParquetReader reader, string filePath);

    IIndexedPredicateConstraint? BuildBinaryConstraint(ExpressionType nodeType, Expression valueExpression);

    IIndexedPredicateConstraint CreateAlwaysMatchConstraint();
}
