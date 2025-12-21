using System.Linq.Expressions;
using System.Reflection;

namespace ParquetSharpLINQ.Query;

/// <summary>
/// Analyzes LINQ expression trees to extract query optimization hints.
/// </summary>
internal sealed class QueryAnalyzer
{
    /// <summary>
    /// Columns explicitly requested via SELECT projection.
    /// - null: No SELECT projection found, read all entity columns
    /// - non-null: Explicit SELECT projection, read only these specific columns
    /// </summary>
    public HashSet<string>? RequestedColumns { get; private set; }
    
    public List<QueryPredicate> Predicates { get; } = [];
    
    /// <summary>
    /// Range filters extracted from WHERE predicates for statistics-based pruning.
    /// Maps column name to (min, max) constraints.
    /// </summary>
    public Dictionary<string, RangeFilter> RangeFilters { get; } = new(StringComparer.OrdinalIgnoreCase);
    
    private bool _isInSelectProjection;

    public static QueryAnalyzer Analyze(Expression expression)
    {
        var analyzer = new QueryAnalyzer();
        analyzer.AnalyzeExpression(expression);
        return analyzer;
    }

    private void AnalyzeExpression(Expression expression)
    {
        switch (expression)
        {
            case MethodCallExpression methodCall:
                AnalyzeMethodCall(methodCall);
                break;
            case UnaryExpression unary:
                AnalyzeExpression(unary.Operand);
                break;
            case BinaryExpression binary:
                AnalyzeBinaryExpression(binary);
                break;
            case MemberExpression member:
                AnalyzeMemberAccess(member);
                break;
            case LambdaExpression lambda:
                AnalyzeExpression(lambda.Body);
                break;
            case InvocationExpression invocation:
                AnalyzeExpression(invocation.Expression);
                foreach (var arg in invocation.Arguments)
                    AnalyzeExpression(arg);
                break;
        }
    }

    private void AnalyzeMethodCall(MethodCallExpression methodCall)
    {
        if (methodCall.Object != null)
            AnalyzeExpression(methodCall.Object);

        foreach (var arg in methodCall.Arguments)
            AnalyzeExpression(arg);

        if (methodCall.Method.Name == "Select" && methodCall.Arguments.Count >= 2)
        {
            var selectorArg = methodCall.Arguments[1];
            if (selectorArg is UnaryExpression { Operand: LambdaExpression quotedLambda })
                AnalyzeSelectProjection(quotedLambda);
            else if (selectorArg is LambdaExpression directLambda)
                AnalyzeSelectProjection(directLambda);
        }

        if (IsPredicateMethod(methodCall.Method.Name) && methodCall.Arguments.Count >= 2)
        {
            var predicateArg = methodCall.Arguments[1];
            if (predicateArg is UnaryExpression { Operand: LambdaExpression quotedLambda })
                AnalyzeWhereClause(quotedLambda);
            else if (predicateArg is LambdaExpression directLambda)
                AnalyzeWhereClause(directLambda);
        }
    }

    private static bool IsPredicateMethod(string methodName)
    {
        return methodName is "Where"
            or "Count" or "LongCount"
            or "Any" or "All"
            or "First" or "FirstOrDefault"
            or "Single" or "SingleOrDefault"
            or "Last" or "LastOrDefault";
    }

    private void AnalyzeSelectProjection(LambdaExpression lambda)
    {
        // Initialize RequestedColumns when we find a SELECT - marks explicit column projection
        RequestedColumns ??= new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        
        _isInSelectProjection = true;
        try
        {
            switch (lambda.Body)
            {
                case NewExpression newExpr:
                    foreach (var arg in newExpr.Arguments)
                        if (arg is MemberExpression member)
                            AnalyzeMemberAccess(member);
                    break;
                case MemberInitExpression initExpr:
                    foreach (var binding in initExpr.Bindings)
                        if (binding is MemberAssignment assignment)
                            AnalyzeExpression(assignment.Expression);
                    break;
                case MemberExpression member:
                    AnalyzeMemberAccess(member);
                    break;
                default:
                    AnalyzeExpression(lambda.Body);
                    break;
            }
        }
        finally
        {
            _isInSelectProjection = false;
        }
    }

    private void AnalyzeWhereClause(LambdaExpression lambda)
    {
        if (lambda.Body.Type != typeof(bool))
            throw new NotSupportedException("Predicate must return a boolean value.");

        Predicates.Add(new QueryPredicate(lambda, QueryPredicatePropertyCollector.Collect(lambda)));
        AnalyzePredicateForRangeFilters(lambda.Body);
    }

    private void AnalyzePredicateForRangeFilters(Expression expression)
    {
        if (expression is not BinaryExpression binary)
            return;

        if (binary.NodeType == ExpressionType.AndAlso)
        {
            AnalyzePredicateForRangeFilters(binary.Left);
            AnalyzePredicateForRangeFilters(binary.Right);
            return;
        }

        if (binary.NodeType == ExpressionType.OrElse)
        {
            AnalyzeBinaryExpression(binary);
            return;
        }

        if (binary.NodeType == ExpressionType.NotEqual)
        {
            AnalyzeBinaryExpression(binary);
            return;
        }

        // Check if this is a string.Compare method call pattern
        if (binary.Left is MethodCallExpression methodCall)
        {
            ExtractStringCompareRangeFilter(methodCall, binary.NodeType, binary.Right);
        }
        else if (binary.Right is MethodCallExpression rightMethodCall)
        {
            // Handle reversed: 0 >= string.Compare(...)
            var reversedType = ReverseComparisonType(binary.NodeType);
            ExtractStringCompareRangeFilter(rightMethodCall, reversedType, binary.Left);
        }

        // Extract range filters for statistics-based pruning (regular property comparisons)
        RangeFilterExtractor.ExtractFromBinaryExpression(binary, RangeFilters);

        AnalyzeBinaryExpression(binary);
    }

    private static ExpressionType ReverseComparisonType(ExpressionType type)
    {
        return type switch
        {
            ExpressionType.GreaterThan => ExpressionType.LessThan,
            ExpressionType.GreaterThanOrEqual => ExpressionType.LessThanOrEqual,
            ExpressionType.LessThan => ExpressionType.GreaterThan,
            ExpressionType.LessThanOrEqual => ExpressionType.GreaterThanOrEqual,
            _ => type
        };
    }

    /// <summary>
    /// Extracts range filter from string.Compare method calls.
    /// Handles patterns like: string.Compare(x.Name, "value") &gt;= 0
    /// </summary>
    private void ExtractStringCompareRangeFilter(
        MethodCallExpression methodCall,
        ExpressionType comparisonType,
        Expression comparisonTarget)
    {
        // Check if this is string.Compare
        if (methodCall.Method.DeclaringType != typeof(string) || methodCall.Method.Name != "Compare")
            return;

        if (methodCall.Arguments.Count < 2)
            return;

        // Try to extract property and comparison value
        string? propertyName = null;
        object? value = null;
        bool isPropertyFirst;

        // Pattern: string.Compare(x.Property, "value") or string.Compare(x.Property, variable)
        if (methodCall.Arguments[0] is MemberExpression leftMember && leftMember.Member is PropertyInfo)
        {
            propertyName = leftMember.Member.Name;
            value = TryEvaluateExpression(methodCall.Arguments[1]);
            isPropertyFirst = true;
        }
        // Pattern: string.Compare("value", x.Property) or string.Compare(variable, x.Property)
        else if (methodCall.Arguments[1] is MemberExpression rightMember && rightMember.Member is PropertyInfo)
        {
            propertyName = rightMember.Member.Name;
            value = TryEvaluateExpression(methodCall.Arguments[0]);
            isPropertyFirst = false;
        }
        else
        {
            return;
        }

        if (value is not string)
            return;

        // Evaluate the comparison value (should be 0 for string.Compare patterns)
        var compareToValue = TryEvaluateExpression(comparisonTarget);
        if (compareToValue is not int compareToInt || compareToInt != 0)
            return; // Only handle comparison to 0

        if (comparisonType != ExpressionType.NotEqual)
        {
            var filter = RangeFilters.GetOrAdd(propertyName, _ => new RangeFilter());

            // Apply the comparison logic based on operand order
            if (isPropertyFirst)
            {
                ApplyPropertyFirstComparison(filter, value, comparisonType);
            }
            else
            {
                ApplyPropertySecondComparison(filter, value, comparisonType);
            }
        }
    }

    /// <summary>
    /// Applies range filter when property is the first argument in string.Compare.
    /// Pattern: string.Compare(x.Property, "value") [operator] 0
    /// </summary>
    private static void ApplyPropertyFirstComparison(RangeFilter filter, object value, ExpressionType comparisonType)
    {
        // string.Compare returns: < 0 if first < second, = 0 if equal, > 0 if first > second
        // So: string.Compare(x.Name, "M") >= 0 means x.Name >= "M"
        switch (comparisonType)
        {
            case ExpressionType.GreaterThanOrEqual:
                // string.Compare(x.Name, "M") &gt;= 0 means x.Name &gt;= "M"
                filter.Min = value;
                filter.MinInclusive = true;
                break;
            case ExpressionType.GreaterThan:
                // string.Compare(x.Name, "M") &gt; 0 means x.Name &gt; "M"
                filter.Min = value;
                filter.MinInclusive = false;
                break;
            case ExpressionType.LessThanOrEqual:
                // string.Compare(x.Name, "Z") &lt;= 0 means x.Name &lt;= "Z"
                filter.Max = value;
                filter.MaxInclusive = true;
                break;
            case ExpressionType.LessThan:
                // string.Compare(x.Name, "Z") &lt; 0 means x.Name &lt; "Z"
                filter.Max = value;
                filter.MaxInclusive = false;
                break;
            case ExpressionType.Equal:
                // string.Compare(x.Name, "Bob") == 0 means x.Name == "Bob"
                filter.Min = value;
                filter.Max = value;
                filter.MinInclusive = true;
                filter.MaxInclusive = true;
                break;
        }
    }

    /// <summary>
    /// Applies range filter when property is the second argument in string.Compare.
    /// Pattern: string.Compare("value", x.Property) [operator] 0
    /// The comparison is reversed: string.Compare("M", x.Name) &gt;= 0 means "M" &gt;= x.Name, so x.Name &lt;= "M"
    /// </summary>
    private static void ApplyPropertySecondComparison(RangeFilter filter, object value, ExpressionType comparisonType)
    {
        switch (comparisonType)
        {
            case ExpressionType.GreaterThanOrEqual:
                // string.Compare("M", x.Name) &gt;= 0 means "M" &gt;= x.Name, so x.Name &lt;= "M"
                filter.Max = value;
                filter.MaxInclusive = true;
                break;
            case ExpressionType.GreaterThan:
                // string.Compare("M", x.Name) &gt; 0 means "M" &gt; x.Name, so x.Name &lt; "M"
                filter.Max = value;
                filter.MaxInclusive = false;
                break;
            case ExpressionType.LessThanOrEqual:
                // string.Compare("Z", x.Name) &lt;= 0 means "Z" &lt;= x.Name, so x.Name &gt;= "Z"
                filter.Min = value;
                filter.MinInclusive = true;
                break;
            case ExpressionType.LessThan:
                // string.Compare("Z", x.Name) &lt; 0 means "Z" &lt; x.Name, so x.Name &gt; "Z"
                filter.Min = value;
                filter.MinInclusive = false;
                break;
            case ExpressionType.Equal:
                // string.Compare("Bob", x.Name) == 0 means "Bob" == x.Name
                filter.Min = value;
                filter.Max = value;
                filter.MinInclusive = true;
                filter.MaxInclusive = true;
                break;
        }
    }

    /// <summary>
    /// Attempts to evaluate an expression to extract its runtime value.
    /// Handles closure members, fields, and other compile-time constant expressions.
    /// </summary>
    private static object? TryEvaluateExpression(Expression expression)
    {
        try
        {
            if (expression is ConstantExpression constant)
            {
                return constant.Value;
            }

            var lambda = Expression.Lambda<Func<object?>>(
                Expression.Convert(expression, typeof(object)));
            var compiled = lambda.Compile();
            return compiled();
        }
        catch
        {
            return null;
        }
    }

    private void AnalyzeBinaryExpression(BinaryExpression binary)
    {
        AnalyzeExpression(binary.Left);
        AnalyzeExpression(binary.Right);
    }

    private void AnalyzeMemberAccess(MemberExpression member)
    {
        // Only add to RequestedColumns when explicitly inside a SELECT projection
        if (_isInSelectProjection && member.Member is PropertyInfo property)
            RequestedColumns?.Add(property.Name);

        if (member.Expression != null)
            AnalyzeExpression(member.Expression);
    }
}
