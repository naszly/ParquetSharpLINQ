using System.Linq.Expressions;
using System.Reflection;
using ParquetSharpLINQ.Interfaces;
using ParquetSharpLINQ.Query;

namespace ParquetSharpLINQ.Enumeration.Indexing;

internal sealed class IndexedPredicateEngine<T> where T : new()
{
    private readonly IParquetReader _parquetReader;
    private readonly IndexedColumnIndexStore _indexedColumnIndexStore = new();

    public IndexedPredicateEngine(IParquetReader parquetReader)
    {
        _parquetReader = parquetReader ?? throw new ArgumentNullException(nameof(parquetReader));
    }

    public IReadOnlyList<IndexedPredicateConstraint> BuildIndexedPredicateConstraints(
        IReadOnlyCollection<QueryPredicate>? predicates,
        bool allowWarmup)
    {
        if (predicates == null || predicates.Count == 0)
            return [];

        var constraints = new List<IndexedPredicateConstraint>();
        var warmupColumns = allowWarmup
            ? new HashSet<string>(StringComparer.OrdinalIgnoreCase)
            : null;

        foreach (var predicate in predicates)
        {
            if (predicate.Expression.Parameters.Count != 1)
                continue;

            if (!TryBuildIndexedConstraintsFromExpression(predicate.Expression.Body, predicate.Expression.Parameters[0],
                    out var predicateConstraints))
            {
                if (warmupColumns != null)
                    AddWarmupConstraints(predicate, warmupColumns);
                continue;
            }

            constraints.AddRange(predicateConstraints);
        }

        if (warmupColumns is { Count: > 0 })
        {
            foreach (var propertyName in warmupColumns)
            {
                if (PropertyColumnMapper<T>.TryGetIndexedColumnDefinition(propertyName, out var definition) &&
                    definition != null)
                {
                    constraints.Add(new AlwaysMatchConstraint(definition));
                }
            }
        }

        return constraints;
    }

    public IReadOnlySet<int>? ResolveRowGroupsToRead(
        string filePath,
        IReadOnlyList<IndexedPredicateConstraint> constraints)
    {
        if (constraints.Count == 0)
            return null;

        HashSet<int>? rowGroupsToRead = null;

        foreach (var constraint in constraints)
        {
            var rowGroupMatches = GetRowGroupsMatchingConstraint(filePath, constraint);
            if (rowGroupsToRead == null)
            {
                rowGroupsToRead = rowGroupMatches;
            }
            else
            {
                rowGroupsToRead.IntersectWith(rowGroupMatches);
            }

            if (rowGroupsToRead.Count == 0)
                return rowGroupsToRead;
        }

        return rowGroupsToRead;
    }

    public IEnumerable<SortedValueArray> GetRowGroupValues(string filePath, IndexedColumnDefinition definition)
    {
        var fileIndex = GetOrCreateRowGroupIndex(filePath, definition);
        return fileIndex.RowGroups.Values;
    }

    public void ClearCache()
    {
        _indexedColumnIndexStore.Clear();
    }

    private HashSet<int> GetRowGroupsMatchingConstraint(
        string filePath,
        IndexedPredicateConstraint constraint)
    {
        var definition = constraint.Definition;
        var fileIndex = GetOrCreateRowGroupIndex(filePath, definition);

        var matches = new HashSet<int>();
        foreach (var (rowGroupIndex, values) in fileIndex.RowGroups)
        {
            if (constraint.RowGroupMayMatch(values))
                matches.Add(rowGroupIndex);
        }

        return matches;
    }

    private RowGroupIndex GetOrCreateRowGroupIndex(string filePath, IndexedColumnDefinition definition)
    {
        var columnIndex = _indexedColumnIndexStore.GetOrAddColumn(definition.ColumnName);
        return columnIndex.GetOrAddFile(filePath, () => BuildRowGroupIndex(filePath, definition));
    }

    private RowGroupIndex BuildRowGroupIndex(string filePath, IndexedColumnDefinition definition)
    {
        var rowGroupValues = _parquetReader.ReadColumnValuesByRowGroup(filePath, definition.ColumnName);
        var rowGroups = new Dictionary<int, SortedValueArray>();

        for (var rowGroupIndex = 0; rowGroupIndex < rowGroupValues.Count; rowGroupIndex++)
        {
            var converted = ConvertIndexValues(rowGroupValues[rowGroupIndex], definition);
            rowGroups[rowGroupIndex] = new SortedValueArray(converted, definition.Comparer);
        }

        return new RowGroupIndex(rowGroups);
    }

    private static IEnumerable<object?> ConvertIndexValues(
        IEnumerable<object?> rawValues,
        IndexedColumnDefinition definition)
    {
        foreach (var raw in rawValues)
        {
            var converted = definition.Converter(raw);
            if (converted == null && !definition.IsNullable)
            {
                throw new InvalidOperationException(
                    $"Indexed column '{definition.ColumnName}' is null for non-nullable property '{definition.Property.Name}'.");
            }

            yield return converted;
        }
    }

    private static void AddWarmupConstraints(
        QueryPredicate predicate,
        HashSet<string> warmupColumns)
    {
        foreach (var property in predicate.Properties)
        {
            if (PropertyColumnMapper<T>.TryGetIndexedColumnDefinition(property.Name, out var definition) &&
                definition != null)
            {
                warmupColumns.Add(property.Name);
            }
        }
    }

    private static bool TryBuildIndexedConstraintsFromExpression(
        Expression expression,
        ParameterExpression parameter,
        out IReadOnlyList<IndexedPredicateConstraint> constraints)
    {
        if (expression is BinaryExpression { NodeType: ExpressionType.AndAlso } andAlso)
        {
            if (!TryBuildIndexedConstraintsFromExpression(andAlso.Left, parameter, out var left) ||
                !TryBuildIndexedConstraintsFromExpression(andAlso.Right, parameter, out var right))
            {
                constraints = [];
                return false;
            }

            constraints = left.Concat(right).ToArray();
            return true;
        }

        var constraint = TryBuildIndexedConstraint(expression, parameter);
        if (constraint == null)
        {
            constraints = [];
            return false;
        }

        constraints = [constraint];
        return true;
    }

    private static IndexedPredicateConstraint? TryBuildIndexedConstraint(
        Expression expression,
        ParameterExpression parameter)
    {
        return expression switch
        {
            BinaryExpression binary => TryBuildIndexedConstraintFromBinary(binary, parameter),
            MethodCallExpression methodCall => TryBuildIndexedConstraintFromMethod(methodCall, parameter),
            _ => null
        };
    }

    private static IndexedPredicateConstraint? TryBuildIndexedConstraintFromBinary(
        BinaryExpression binary,
        ParameterExpression parameter)
    {
        if (TryGetPropertyAccess(binary.Left, parameter, out var property) && property != null)
        {
            var valueExpression = binary.Right;
            return BuildBinaryConstraint(binary.NodeType, property, valueExpression);
        }

        if (TryGetPropertyAccess(binary.Right, parameter, out property) && property != null)
        {
            var valueExpression = binary.Left;
            return BuildBinaryConstraint(binary.NodeType, property, valueExpression);
        }

        return null;
    }

    private static IndexedPredicateConstraint? BuildBinaryConstraint(
        ExpressionType nodeType,
        MemberExpression property,
        Expression valueExpression)
    {
        if (!TryEvaluateConstant(valueExpression, out var rawValue))
            return null;

        if (!PropertyColumnMapper<T>.TryGetIndexedColumnDefinition(property.Member.Name, out var definition) ||
            definition == null)
            return null;

        var converted = rawValue == null ? null : definition.Converter(rawValue);
        if (converted == null && !definition.IsNullable)
            return null;
        return nodeType switch
        {
            ExpressionType.Equal => new EqualsConstraint(definition, converted),
            ExpressionType.NotEqual => new NotEqualsConstraint(definition, converted),
            ExpressionType.GreaterThan => new ComparisonConstraint(definition, converted, ComparisonKind.GreaterThan),
            ExpressionType.GreaterThanOrEqual => new ComparisonConstraint(definition, converted, ComparisonKind.GreaterThanOrEqual),
            ExpressionType.LessThan => new ComparisonConstraint(definition, converted, ComparisonKind.LessThan),
            ExpressionType.LessThanOrEqual => new ComparisonConstraint(definition, converted, ComparisonKind.LessThanOrEqual),
            _ => null
        };
    }

    private static IndexedPredicateConstraint? TryBuildIndexedConstraintFromMethod(
        MethodCallExpression methodCall,
        ParameterExpression parameter)
    {
        if (methodCall.Method.Name != nameof(string.StartsWith) || methodCall.Object == null)
            return null;

        if (!IsSupportedStartsWithComparison(methodCall))
            return null;

        if (!TryGetPropertyAccess(methodCall.Object, parameter, out var property) || property == null)
            return null;

        if (property.Member is not PropertyInfo propertyInfo || propertyInfo.PropertyType != typeof(string))
            return null;

        if (!TryEvaluateConstant(methodCall.Arguments[0], out var rawValue) || rawValue is not string prefix)
            return null;

        if (!PropertyColumnMapper<T>.TryGetIndexedColumnDefinition(propertyInfo.Name, out var definition) ||
            definition == null)
            return null;

        return StartsWithConstraint.TryCreate(definition, prefix);
    }

    private static bool IsSupportedStartsWithComparison(MethodCallExpression methodCall)
    {
        if (methodCall.Arguments.Count == 1)
            return true;

        if (methodCall.Arguments.Count != 2)
            return false;

        if (!TryEvaluateConstant(methodCall.Arguments[1], out var comparison) ||
            comparison is not StringComparison comparisonValue)
            return false;

        return comparisonValue == StringComparison.Ordinal;
    }

    private static bool TryGetPropertyAccess(
        Expression expression,
        ParameterExpression parameter,
        out MemberExpression? memberExpression)
    {
        memberExpression = expression as MemberExpression ?? (expression as UnaryExpression)?.Operand as MemberExpression;
        if (memberExpression == null)
            return false;

        return ExpressionHelpers.IsParameterMember(memberExpression, parameter);
    }

    private static bool TryEvaluateConstant(Expression expression, out object? value)
    {
        if (expression is ConstantExpression constant)
        {
            value = constant.Value;
            return true;
        }

        if (expression is UnaryExpression { NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked } unary)
            return TryEvaluateConstant(unary.Operand, out value);

        if (ContainsParameterReference(expression))
        {
            value = null;
            return false;
        }

        value = Expression.Lambda(expression).Compile().DynamicInvoke();
        return true;
    }

    private static bool ContainsParameterReference(Expression expression)
    {
        var visitor = new ParameterDetectingVisitor();
        visitor.Visit(expression);
        return visitor.HasParameter;
    }

    private sealed class ParameterDetectingVisitor : ExpressionVisitor
    {
        public bool HasParameter { get; private set; }

        protected override Expression VisitParameter(ParameterExpression node)
        {
            HasParameter = true;
            return base.VisitParameter(node);
        }
    }
}
