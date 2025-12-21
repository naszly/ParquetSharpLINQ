using System.Collections.Immutable;
using System.Linq.Expressions;
using System.Reflection;
using ParquetSharpLINQ.Constants;
using ParquetSharpLINQ.Mappers;
using ParquetSharpLINQ.ParquetSharp;
using ParquetSharpLINQ.Query;

namespace ParquetSharpLINQ.Enumeration;

internal static class RowPredicateBuilder<T> where T : new()
{
    public static IReadOnlyList<Func<ParquetRow, bool>> BuildRowPredicates(
        IReadOnlyCollection<QueryPredicate>? predicates)
    {
        if (predicates == null || predicates.Count == 0)
            return [];

        var propertyToColumnMap = PropertyColumnMapper<T>.GetPropertyToColumnMap();
        var partitionProperties = PropertyColumnMapper<T>.GetPartitionPropertyNames();

        return predicates
            .Select(predicate => TryBuildRowPredicateEvaluator(predicate.Expression, propertyToColumnMap, partitionProperties))
            .OfType<Func<ParquetRow, bool>>().ToList();
    }

    private static Func<ParquetRow, bool>? TryBuildRowPredicateEvaluator(
        LambdaExpression expression,
        IReadOnlyDictionary<string, string> propertyToColumnMap,
        IImmutableSet<string> partitionProperties)
    {
        if (expression.Parameters.Count != 1)
            return null;

        var rowParameter = Expression.Parameter(typeof(ParquetRow), "row");
        var entityParameter = expression.Parameters[0];

        var rewritten = new RowPredicateRewriter(rowParameter, entityParameter, propertyToColumnMap, partitionProperties)
            .Visit(expression.Body);

        if (rewritten.Type != typeof(bool))
            return null;

        return Expression.Lambda<Func<ParquetRow, bool>>(rewritten, rowParameter).Compile();
    }

    private sealed class RowPredicateRewriter : ExpressionVisitor
    {
        private static readonly MethodInfo GetRowValueMethod =
            typeof(RowPredicateBuilder<T>)
                .GetMethod(nameof(GetRowValue), BindingFlags.Static | BindingFlags.NonPublic)!;

        private readonly ParameterExpression _rowParameter;
        private readonly ParameterExpression _entityParameter;
        private readonly IReadOnlyDictionary<string, string> _propertyToColumnMap;
        private readonly IImmutableSet<string> _partitionProperties;

        public RowPredicateRewriter(
            ParameterExpression rowParameter,
            ParameterExpression entityParameter,
            IReadOnlyDictionary<string, string> propertyToColumnMap,
            IImmutableSet<string> partitionProperties)
        {
            _rowParameter = rowParameter;
            _entityParameter = entityParameter;
            _propertyToColumnMap = propertyToColumnMap;
            _partitionProperties = partitionProperties;
        }

        protected override Expression VisitMember(MemberExpression node)
        {
            if (node.Member is PropertyInfo property && ExpressionHelpers.IsParameterMember(node, _entityParameter))
            {
                var columnName = _propertyToColumnMap.TryGetValue(property.Name, out var mapped)
                    ? mapped
                    : property.Name;

                if (_partitionProperties.Contains(property.Name))
                    columnName = $"{PartitionConstants.PartitionPrefix}{columnName}";

                var method = GetRowValueMethod.MakeGenericMethod(property.PropertyType);
                return Expression.Call(method, _rowParameter, Expression.Constant(columnName));
            }

            return base.VisitMember(node);
        }
    }

    private static TValue GetRowValue<TValue>(ParquetRow row, string columnName)
    {
        return ParquetMapperHelpers.TryGetValue(row, columnName, out var raw)
            ? ParquetMapperHelpers.ConvertValue<TValue>(raw)
            : default!;
    }
}
