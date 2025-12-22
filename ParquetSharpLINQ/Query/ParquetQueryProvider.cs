using System.Collections;
using System.Linq.Expressions;
using System.Runtime.CompilerServices;

namespace ParquetSharpLINQ.Query;

internal sealed class ParquetQueryProvider<T> : IQueryProvider where T : new()
{
    private readonly ParquetEnumerationStrategy<T> _enumerationStrategy;

    internal ParquetQueryProvider(ParquetEnumerationStrategy<T> enumerationStrategy)
    {
        _enumerationStrategy = enumerationStrategy ?? throw new ArgumentNullException(nameof(enumerationStrategy));
    }

    public IQueryable CreateQuery(Expression expression)
    {
        ArgumentNullException.ThrowIfNull(expression);

        var elementType = GetElementType(expression.Type);
        return new ParquetQueryableAdapter(this, expression, elementType);
    }

    public IQueryable<TElement> CreateQuery<TElement>(Expression expression)
    {
        ArgumentNullException.ThrowIfNull(expression);
        return new ParquetQueryable<TElement>(this, expression);
    }

    public object? Execute(Expression expression)
    {
        ArgumentNullException.ThrowIfNull(expression);
        return Execute<object?>(expression);
    }

    public TResult Execute<TResult>(Expression expression)
    {
        ArgumentNullException.ThrowIfNull(expression);

        // Analyze the query to extract optimization hints
        var analysis = QueryAnalyzer.Analyze(expression);
        IReadOnlyCollection<string>? columnsToRead = null;
        if (analysis.SelectedColumns != null)
        {
            var union = new HashSet<string>(analysis.SelectedColumns, StringComparer.OrdinalIgnoreCase);
            union.UnionWith(analysis.AccessedColumns);
            columnsToRead = union;
        }

        if (TryExecuteCountUsingIndex(expression, analysis, out TResult optimizedResult))
            return optimizedResult;

        // Execute query using enumeration strategy with statistics-based pruning
        var sourceQueryable = _enumerationStrategy.Enumerate(
            analysis.Predicates.Count > 0 ? analysis.Predicates : null,
            columnsToRead,
            analysis.RangeFilters.Count > 0 ? analysis.RangeFilters : null
        ).AsQueryable();

        var rewritten = ParquetExpressionReplacer<T>.Replace(expression, sourceQueryable);
        return sourceQueryable.Provider.Execute<TResult>(rewritten);
    }

    private bool TryExecuteCountUsingIndex<TResult>(Expression expression, QueryAnalyzer analysis, out TResult result)
    {
        if (expression is not MethodCallExpression methodCall)
        {
            Unsafe.SkipInit(out result);
            return false;
        }

        var isCount = methodCall.Method.Name is "Count" or "LongCount";
        if (!isCount)
        {
            Unsafe.SkipInit(out result);
            return false;
        }

        if (!_enumerationStrategy.TryCountUsingIndex(analysis.Predicates, analysis.RangeFilters, out var count))
        {
            Unsafe.SkipInit(out result);
            return false;
        }

        if (methodCall.Method.Name == "LongCount")
        {
            result = (TResult)(object)count;
            return true;
        }

        result = (TResult)(object)checked((int)count);
        return true;
    }

    private static Type GetElementType(Type sequenceType)
    {
        if (sequenceType.IsGenericType)
        {
            var definition = sequenceType.GetGenericTypeDefinition();
            if (definition == typeof(IQueryable<>) || definition == typeof(IEnumerable<>))
                return sequenceType.GetGenericArguments()[0];
        }

        var interfaceType = sequenceType.GetInterfaces()
            .FirstOrDefault(i => i.IsGenericType && (i.GetGenericTypeDefinition() == typeof(IQueryable<>) ||
                                                     i.GetGenericTypeDefinition() == typeof(IEnumerable<>)));

        if (interfaceType != null)
            return interfaceType.GetGenericArguments()[0];

        throw new ArgumentException("Expression does not represent a queryable sequence.", nameof(sequenceType));
    }
}

internal sealed class ParquetQueryable<TElement> : IOrderedQueryable<TElement>
{
    public ParquetQueryable(IQueryProvider provider, Expression expression)
    {
        Provider = provider ?? throw new ArgumentNullException(nameof(provider));
        Expression = expression ?? throw new ArgumentNullException(nameof(expression));

        var type = expression.Type;
        if (!typeof(IQueryable<TElement>).IsAssignableFrom(type) &&
            !typeof(IEnumerable<TElement>).IsAssignableFrom(type))
            throw new ArgumentOutOfRangeException(nameof(expression),
                "Expression must represent a sequence of the requested element type.");
    }

    public Type ElementType => typeof(TElement);

    public Expression Expression { get; }

    public IQueryProvider Provider { get; }

    public IEnumerator<TElement> GetEnumerator()
    {
        var result = Provider.Execute<IEnumerable<TElement>>(Expression);
        return result.GetEnumerator();
    }

    IEnumerator IEnumerable.GetEnumerator()
    {
        return GetEnumerator();
    }
}

internal sealed class ParquetQueryableAdapter : IOrderedQueryable
{
    public ParquetQueryableAdapter(IQueryProvider provider, Expression expression, Type elementType)
    {
        Provider = provider ?? throw new ArgumentNullException(nameof(provider));
        Expression = expression ?? throw new ArgumentNullException(nameof(expression));
        ElementType = elementType ?? throw new ArgumentNullException(nameof(elementType));
    }

    public Type ElementType { get; }

    public Expression Expression { get; }

    public IQueryProvider Provider { get; }

    public IEnumerator GetEnumerator()
    {
        var result = Provider.Execute<IEnumerable>(Expression);
        return result.GetEnumerator();
    }
}

internal sealed class ParquetExpressionReplacer<TRoot> : ExpressionVisitor where TRoot : new()
{
    private readonly IQueryable _replacement;

    private ParquetExpressionReplacer(IQueryable replacement)
    {
        _replacement = replacement;
    }

    public static Expression Replace(Expression expression, IQueryable replacement)
    {
        return new ParquetExpressionReplacer<TRoot>(replacement).Visit(expression);
    }

    protected override Expression VisitConstant(ConstantExpression node)
    {
        // Replace any ParquetTable<TRoot> constant with the replacement queryable
        if (node.Value is ParquetTable<TRoot>)
        {
            return Expression.Constant(_replacement, _replacement.GetType());
        }

        return base.VisitConstant(node);
    }
}
