using System.Collections;
using System.Linq.Expressions;

namespace ParquetSharpLINQ;

internal sealed class ParquetQueryProvider<T> : IQueryProvider where T : new()
{
    private readonly ParquetTable<T> _table;

    internal ParquetQueryProvider(ParquetTable<T> table)
    {
        _table = table ?? throw new ArgumentNullException(nameof(table));
    }

    public IQueryable CreateQuery(Expression expression)
    {
        ArgumentNullException.ThrowIfNull(expression);

        var elementType = GetElementType(expression.Type);
        var queryableType = typeof(ParquetQueryable<>).MakeGenericType(elementType);
        return (IQueryable)Activator.CreateInstance(queryableType, this, expression)!;
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

        // Check if we can use async prefetching for Azure Blob Storage
        if (_table.CanUsePrefetch())
        {
            return Task.Run(async () => await ExecuteWithPrefetchAsync<TResult>(
                expression,
                analysis.PartitionFilters.Count > 0 ? analysis.PartitionFilters : null,
                analysis.RequestedColumns
            )).GetAwaiter().GetResult();
        }

        // Fallback to synchronous execution for local files
        var sourceQueryable = _table.AsEnumerable(
            analysis.PartitionFilters.Count > 0 ? analysis.PartitionFilters : null,
            analysis.RequestedColumns
        ).AsQueryable();

        var rewritten = ParquetExpressionReplacer<T>.Replace(expression, _table, sourceQueryable);
        return sourceQueryable.Provider.Execute<TResult>(rewritten);
    }

    private async Task<TResult> ExecuteWithPrefetchAsync<TResult>(
        Expression expression,
        IReadOnlyDictionary<string, object?>? partitionFilters,
        IReadOnlyCollection<string>? requestedColumns)
    {
        // Prefetch blobs asynchronously first
        await _table.PrefetchAsync(partitionFilters, prefetchParallelism: ParquetConfiguration.DefaultPrefetchParallelism);

        // Now execute using the regular synchronous path - blobs are cached
        var sourceQueryable = _table.AsEnumerable(partitionFilters, requestedColumns).AsQueryable();
        var rewritten = ParquetExpressionReplacer<T>.Replace(expression, _table, sourceQueryable);
        return sourceQueryable.Provider.Execute<TResult>(rewritten);
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

internal sealed class ParquetExpressionReplacer<TRoot> : ExpressionVisitor where TRoot : new()
{
    private readonly IQueryable _replacement;
    private readonly object _target;

    private ParquetExpressionReplacer(object target, IQueryable replacement)
    {
        _target = target;
        _replacement = replacement;
    }

    public static Expression Replace(Expression expression, object target, IQueryable replacement)
    {
        ArgumentNullException.ThrowIfNull(expression);
        ArgumentNullException.ThrowIfNull(target);
        ArgumentNullException.ThrowIfNull(replacement);

        return new ParquetExpressionReplacer<TRoot>(target, replacement).Visit(expression);
    }

    protected override Expression VisitConstant(ConstantExpression node)
    {
        if (node.Value == _target) return Expression.Constant(_replacement, _replacement.GetType());

        return base.VisitConstant(node);
    }
}