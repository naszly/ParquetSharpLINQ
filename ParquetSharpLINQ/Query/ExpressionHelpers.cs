using System.Linq.Expressions;

namespace ParquetSharpLINQ.Query
{
    internal static class ExpressionHelpers
    {
        /// <summary>
        /// Determines whether the specified <see cref="MemberExpression"/> accesses the given
        /// <see cref="ParameterExpression"/> either directly (for example, <c>parameter.Property</c>)
        /// or wrapped in a unary conversion (for example, <c>(object)parameter.Property</c>).
        /// </summary>
        /// <param name="node">The member expression to inspect.</param>
        /// <param name="parameter">The parameter expression to match against.</param>
        /// <returns>
        /// True when the member expression targets the provided parameter (possibly via
        /// <see cref="ExpressionType.Convert"/> or <see cref="ExpressionType.ConvertChecked"/>); otherwise false.
        /// </returns>
        public static bool IsParameterMember(MemberExpression node, ParameterExpression parameter)
        {
            if (node.Expression == parameter)
                return true;

            return node.Expression is UnaryExpression
            {
                NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked,
                Operand: ParameterExpression p
            } && p == parameter;
        }
    }
}
