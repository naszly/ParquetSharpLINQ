using System.Linq.Expressions;
using System.Reflection;

namespace ParquetSharpLINQ.Query;

/// <summary>
/// Extracts range filters from LINQ expression trees for statistics-based pruning.
/// Analyzes predicates like x &gt;= 10, x &lt; 100, etc. and builds min/max constraints.
/// </summary>
internal static class RangeFilterExtractor
{
    /// <summary>
    /// Extracts range filters from a binary expression (comparison operators).
    /// Updates the provided dictionary with discovered constraints.
    /// </summary>
    public static void ExtractFromBinaryExpression(
        BinaryExpression binary,
        Dictionary<string, RangeFilter> rangeFilters)
    {
        string? propertyName = null;
        object? value = null;
        bool isLeftMember = false;

        // Determine which side is the property and which is the value
        if (binary.Left is MemberExpression leftMember && leftMember.Member is PropertyInfo)
        {
            propertyName = leftMember.Member.Name;
            value = TryEvaluateExpression(binary.Right);
            isLeftMember = true;
        }
        else if (binary.Right is MemberExpression rightMember && rightMember.Member is PropertyInfo)
        {
            propertyName = rightMember.Member.Name;
            value = TryEvaluateExpression(binary.Left);
            isLeftMember = false;
        }

        if (propertyName == null || value == null)
            return;

        // Skip non-comparable types
        if (!IsComparableType(value))
            return;

        var filter = rangeFilters.GetOrAdd(propertyName, _ => new RangeFilter());

        switch (binary.NodeType)
        {
            case ExpressionType.Equal:
                // x == value means x >= value AND x <= value
                filter.Min = value;
                filter.Max = value;
                filter.MinInclusive = true;
                filter.MaxInclusive = true;
                break;

            case ExpressionType.GreaterThan:
                // x > value (member on left) OR value > x (member on right)
                if (isLeftMember)
                {
                    filter.Min = value;
                    filter.MinInclusive = false;
                }
                else
                {
                    filter.Max = value;
                    filter.MaxInclusive = false;
                }
                break;

            case ExpressionType.GreaterThanOrEqual:
                // x >= value (member on left) OR value >= x (member on right)
                if (isLeftMember)
                {
                    filter.Min = value;
                    filter.MinInclusive = true;
                }
                else
                {
                    filter.Max = value;
                    filter.MaxInclusive = true;
                }
                break;

            case ExpressionType.LessThan:
                // x < value (member on left) OR value < x (member on right)
                if (isLeftMember)
                {
                    filter.Max = value;
                    filter.MaxInclusive = false;
                }
                else
                {
                    filter.Min = value;
                    filter.MinInclusive = false;
                }
                break;

            case ExpressionType.LessThanOrEqual:
                // x <= value (member on left) OR value <= x (member on right)
                if (isLeftMember)
                {
                    filter.Max = value;
                    filter.MaxInclusive = true;
                }
                else
                {
                    filter.Min = value;
                    filter.MinInclusive = true;
                }
                break;
        }
    }

    private static bool IsComparableType(object value)
    {
        return value is int or long or short or byte
            or uint or ulong or ushort or sbyte
            or float or double or decimal
            or DateTime or DateTimeOffset or DateOnly or TimeOnly
            or string;
    }

    private static object? TryEvaluateExpression(Expression expression)
    {
        try
        {
            if (expression is ConstantExpression constant)
                return constant.Value;

            var lambda = Expression.Lambda(expression);
            var compiledDelegate = lambda.Compile();
            var result = compiledDelegate.DynamicInvoke();
            
            return result;
        }
        catch
        {
            return null;
        }
    }
}

