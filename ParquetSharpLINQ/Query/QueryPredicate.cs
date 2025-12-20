using System.Linq.Expressions;
using System.Reflection;

namespace ParquetSharpLINQ.Query;

internal sealed record QueryPredicate(
    LambdaExpression Expression,
    IReadOnlyList<PropertyInfo> Properties);
