using System.Linq.Expressions;
using System.Reflection;

namespace ParquetSharpLINQ.Query;

internal static class QueryPredicatePropertyCollector
{
    public static IReadOnlyList<PropertyInfo> Collect(LambdaExpression lambda)
    {
        var properties = new HashSet<PropertyInfo>();
        var parameter = lambda.Parameters.FirstOrDefault();

        if (parameter == null)
            return [];

        var visitor = new PredicatePropertyCollector(parameter, properties);
        visitor.Visit(lambda.Body);

        return properties.ToArray();
    }

    private sealed class PredicatePropertyCollector : ExpressionVisitor
    {
        private readonly ParameterExpression _parameter;
        private readonly HashSet<PropertyInfo> _properties;

        public PredicatePropertyCollector(ParameterExpression parameter, HashSet<PropertyInfo> properties)
        {
            _parameter = parameter;
            _properties = properties;
        }

        protected override Expression VisitMember(MemberExpression node)
        {
            if (node.Member is PropertyInfo property && ExpressionHelpers.IsParameterMember(node, _parameter))
                _properties.Add(property);

            return base.VisitMember(node);
        }
    }
}
