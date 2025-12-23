using System.Linq.Expressions;
using System.Reflection;
using ParquetSharpLINQ.Mappers;
using ParquetSharpLINQ.ParquetSharp.ParquetRow;
using ParquetSharpLINQ.Query;

namespace ParquetSharpLINQ.Enumeration;

internal static class RowPredicateBuilder<T> where T : new()
{
    public static IReadOnlyList<Func<ParquetRow, bool>> BuildRowPredicates(
        IReadOnlyCollection<QueryPredicate>? predicates)
    {
        if (predicates == null || predicates.Count == 0)
            return [];

        if (!ParquetMapperRegistry.TryGetMetadata(typeof(T), out var metadata) || metadata == null)
        {
            throw new InvalidOperationException(
                $"No generated Parquet metadata found for type {typeof(T).FullName}. " +
                "Ensure the type has [ParquetColumn] attributes and the project is built.");
        }

        var accessors = metadata.PropertyAccessors;

        return predicates
            .Select(predicate => TryBuildRowPredicateEvaluator(predicate.Expression, accessors))
            .OfType<Func<ParquetRow, bool>>().ToList();
    }

    private static Func<ParquetRow, bool>? TryBuildRowPredicateEvaluator(
        LambdaExpression expression,
        IReadOnlyDictionary<string, Delegate> accessors)
    {
        if (expression.Parameters.Count != 1)
            return null;

        var rowParameter = Expression.Parameter(typeof(ParquetRow), "row");
        var entityParameter = expression.Parameters[0];

        var rewritten = new RowPredicateRewriter(rowParameter, entityParameter, accessors)
            .Visit(expression.Body);

        if (rewritten.Type != typeof(bool))
            return null;

        return Expression.Lambda<Func<ParquetRow, bool>>(rewritten, rowParameter).Compile();
    }

    private sealed class RowPredicateRewriter : ExpressionVisitor
    {
        private readonly ParameterExpression _rowParameter;
        private readonly ParameterExpression _entityParameter;
        private readonly IReadOnlyDictionary<string, Delegate> _accessors;

        public RowPredicateRewriter(
            ParameterExpression rowParameter,
            ParameterExpression entityParameter,
            IReadOnlyDictionary<string, Delegate> accessors)
        {
            _rowParameter = rowParameter;
            _entityParameter = entityParameter;
            _accessors = accessors;
        }

        protected override Expression VisitMember(MemberExpression node)
        {
            if (node.Member is PropertyInfo property && ExpressionHelpers.IsParameterMember(node, _entityParameter))
            {
                if (_accessors.TryGetValue(property.Name, out var accessor))
                {
                    var accessorExpr = Expression.Constant(accessor, accessor.GetType());
                    return Expression.Invoke(accessorExpr, _rowParameter);
                }
            }

            return base.VisitMember(node);
        }
    }
}
