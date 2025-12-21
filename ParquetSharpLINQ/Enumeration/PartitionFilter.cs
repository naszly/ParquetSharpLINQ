using System.Linq.Expressions;
using ParquetSharpLINQ.Models;
using ParquetSharpLINQ.Query;
using System.Collections.Immutable;
using System.Reflection;

namespace ParquetSharpLINQ.Enumeration;

internal static partial class PartitionFilter
{
    public static IEnumerable<Partition> PrunePartitions<T>(
        IEnumerable<Partition> partitions,
        IReadOnlyCollection<QueryPredicate> predicates)
        where T : new()
    {
        if (predicates.Count == 0)
            return partitions;

        var partitionProperties = PropertyColumnMapper<T>.GetPartitionPropertyNames();
        var propertyToColumn = PropertyColumnMapper<T>.GetPropertyToColumnMap();
        var applicablePredicates = predicates
            .Where(p => p.Expression.Parameters.Count == 1)
            .ToArray();

        if (applicablePredicates.Length == 0)
            return partitions;

        return partitions.Where(p => PartitionMatchesAllPredicates(
            p,
            applicablePredicates,
            partitionProperties,
            propertyToColumn));
    }

    private static bool PartitionMatchesAllPredicates(
        Partition partition,
        IReadOnlyCollection<QueryPredicate> predicates,
        IImmutableSet<string> partitionProperties,
        IReadOnlyDictionary<string, string> propertyToColumn)
    {
        foreach (var predicate in predicates)
        {
            if (!TryEvaluatePredicate(partition, predicate, partitionProperties, propertyToColumn, out var result))
                continue;

            if (!result)
                return false;
        }

        return true;
    }

    private static bool TryEvaluatePredicate(
        Partition partition,
        QueryPredicate predicate,
        IImmutableSet<string> partitionProperties,
        IReadOnlyDictionary<string, string> propertyToColumn,
        out bool result)
    {
        if (!IsPartitionPredicate(predicate, partitionProperties))
        {
            result = true;
            return false;
        }

        var parameter = predicate.Expression.Parameters[0];
        var evaluator = new PartitionPredicateEvaluator(
            partition,
            parameter,
            partitionProperties,
            propertyToColumn);

        var rewritten = evaluator.Visit(predicate.Expression.Body);

        if (evaluator.HasNonPartitionAccess || evaluator.HasMissingValues)
        {
            result = true;
            return false;
        }

        if (rewritten.Type != typeof(bool))
            throw new NotSupportedException($"Partition predicate must return a boolean value, got {rewritten.Type}.");

        var lambda = Expression.Lambda<Func<bool>>(rewritten);
        result = lambda.Compile().Invoke();
        return true;
    }

    private sealed class PartitionPredicateEvaluator : ExpressionVisitor
    {
        private readonly Partition _partition;
        private readonly ParameterExpression _parameter;
        private readonly IImmutableSet<string> _partitionProperties;
        private readonly IReadOnlyDictionary<string, string> _propertyToColumn;

        public bool HasMissingValues { get; private set; }
        public bool HasNonPartitionAccess { get; private set; }

        public PartitionPredicateEvaluator(
            Partition partition,
            ParameterExpression parameter,
            IImmutableSet<string> partitionProperties,
            IReadOnlyDictionary<string, string> propertyToColumn)
        {
            _partition = partition;
            _parameter = parameter;
            _partitionProperties = partitionProperties;
            _propertyToColumn = propertyToColumn;
        }

        protected override Expression VisitMember(MemberExpression node)
        {
            if (node.Member is PropertyInfo property && ExpressionHelpers.IsParameterMember(node, _parameter))
            {
                if (!_partitionProperties.Contains(property.Name))
                {
                    HasNonPartitionAccess = true;
                    return Expression.Default(property.PropertyType);
                }

                var columnName = _propertyToColumn.TryGetValue(property.Name, out var mapped)
                    ? mapped
                    : property.Name;

                if (!_partition.Values.TryGetValue(columnName, out var rawValue))
                {
                    HasMissingValues = true;
                    return Expression.Default(property.PropertyType);
                }

                var converted = PartitionValueConverter.Convert(rawValue, property.PropertyType);
                return Expression.Constant(converted, property.PropertyType);
            }

            return base.VisitMember(node);
        }

        protected override Expression VisitConstant(ConstantExpression node)
        {
            if (node.Value is string stringValue && node.Type == typeof(string))
            {
                var normalized = FilterValueNormalizer.NormalizePartitionValue(stringValue);
                return Expression.Constant(normalized, typeof(string));
            }

            return base.VisitConstant(node);
        }
    }

    private static bool IsPartitionPredicate(QueryPredicate predicate, IImmutableSet<string> partitionProperties)
    {
        if (predicate.Properties.Count == 0)
            return false;

        return predicate.Properties.All(property => partitionProperties.Contains(property.Name));
    }
}
