using System.Linq.Expressions;
using System.Reflection;

namespace ParquetSharpLINQ;

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
    
    public Dictionary<string, object?> PartitionFilters { get; } = new(StringComparer.OrdinalIgnoreCase);
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
            if (selectorArg is UnaryExpression { Operand: LambdaExpression lambda })
                AnalyzeSelectProjection(lambda);
        }

        if (IsPredicateMethod(methodCall.Method.Name) && methodCall.Arguments.Count >= 2)
        {
            var predicateArg = methodCall.Arguments[1];
            if (predicateArg is UnaryExpression { Operand: LambdaExpression lambda })
                AnalyzeWhereClause(lambda);
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
        AnalyzePredicateForPartitionFilters(lambda.Body);
    }

    private void AnalyzePredicateForPartitionFilters(Expression expression)
    {
        if (expression is not BinaryExpression binary) return;

        if (binary.NodeType == ExpressionType.AndAlso)
        {
            AnalyzePredicateForPartitionFilters(binary.Left);
            AnalyzePredicateForPartitionFilters(binary.Right);
            return;
        }

        if (binary.NodeType == ExpressionType.Equal)
        {
            string? propertyName = null;
            object? value = null;

            switch (binary)
            {
                case { Left: MemberExpression leftMember, Right: ConstantExpression rightConst }:
                    propertyName = leftMember.Member.Name;
                    value = rightConst.Value;
                    break;
                case { Right: MemberExpression rightMember, Left: ConstantExpression leftConst }:
                    propertyName = rightMember.Member.Name;
                    value = leftConst.Value;
                    break;
                case { Left: MemberExpression leftMember }:
                    propertyName = leftMember.Member.Name;
                    value = TryEvaluateExpression(binary.Right);
                    break;
                case { Right: MemberExpression rightMember }:
                    propertyName = rightMember.Member.Name;
                    value = TryEvaluateExpression(binary.Left);
                    break;
            }

            if (propertyName != null)
                PartitionFilters.TryAdd(propertyName, value);
        }

        AnalyzeBinaryExpression(binary);
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