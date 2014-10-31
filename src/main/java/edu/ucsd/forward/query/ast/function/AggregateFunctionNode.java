package edu.ucsd.forward.query.ast.function;

import java.util.List;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.source.DataSource;
import edu.ucsd.forward.exception.Location;
import edu.ucsd.forward.query.QueryCompilationException;
import edu.ucsd.forward.query.ast.AstNode;
import edu.ucsd.forward.query.ast.SetQuantifier;
import edu.ucsd.forward.query.ast.ValueExpression;
import edu.ucsd.forward.query.ast.visitors.AstNodeVisitor;
import edu.ucsd.forward.query.explain.QueryExpressionPrinter;

/**
 * An aggregate function call, which has a name, a set quantifier and an argument. The argument is a child AST node of the function
 * call node.
 * 
 * @author Michalis Petropoulos
 * 
 */
public class AggregateFunctionNode extends AbstractFunctionNode
{
    @SuppressWarnings("unused")
    private static final Logger log              = Logger.getLogger(AggregateFunctionNode.class);
    
    private static final String ASTERISK         = "*";
    
    private AggregateType       m_func_name;
    
    private SetQuantifier       m_set_quantifier = SetQuantifier.ALL;
    
    /**
     * The aggregate types.
     * 
     * @author Michalis Petropoulos
     * 
     */
    public enum AggregateType
    {
        COUNT, AVG, MAX, MIN, SUM, NEST, STRING_AGG;
    }
    
    /**
     * Constructs the COUNT(*) aggregate function call.
     * 
     * @param name
     *            the function call name.
     * @param location
     *            a location.
     */
    public AggregateFunctionNode(AggregateType name, Location location)
    {
        super(name.name(), location);
        
        assert (name != null);
        m_func_name = name;
    }
    
    /**
     * Constructs the aggregate function call with name, set quantifier and argument.
     * 
     * @param name
     *            the function name.
     * @param set_quantifier
     *            a set quantifier.
     * @param arguments
     *            the arguments.
     * @param location
     *            a location.
     */
    public AggregateFunctionNode(AggregateType name, SetQuantifier set_quantifier, List<ValueExpression> arguments,
            Location location)
    {
        this(name, location);
        
        assert (set_quantifier != null);
        m_set_quantifier = set_quantifier;
        
        for (ValueExpression argument : arguments)
        {
            addArgument(argument);
        }
    }
    
    /**
     * Gets the set quantifier.
     * 
     * @return the set quantification.
     */
    public SetQuantifier getSetQuantifier()
    {
        return m_set_quantifier;
    }
    
    /**
     * Sets the set quantifier.
     * 
     * @param set_quantifier
     *            a set quantifier.
     */
    public void setSetQuantifier(SetQuantifier set_quantifier)
    {
        assert (set_quantifier != null);
        
        m_set_quantifier = set_quantifier;
    }
    
    @Override
    public void toQueryString(StringBuilder sb, int tabs, DataSource data_source)
    {
        sb.append(this.getIndent(tabs));
        
        sb.append(m_func_name + QueryExpressionPrinter.LEFT_PAREN);
        
        if (!getArguments().isEmpty())
        {
            sb.append(m_set_quantifier.name() + " ");
            for (ValueExpression argument : getArguments())
            {
                argument.toQueryString(sb, 0, data_source);
                sb.append(", ");
            }
            sb.delete(sb.length() - 2, sb.length());
        }
        // Accommodate COUNT(*)
        else sb.append(ASTERISK);
        
        sb.append(QueryExpressionPrinter.RIGHT_PAREN);
    }
    
    @Override
    public String toExplainString()
    {
        StringBuilder out = new StringBuilder(super.toExplainString());
        out.append(" -> ");
        this.toQueryString(out, 0, null);
        
        return out.toString();
    }
    
    @Override
    public Object accept(AstNodeVisitor visitor) throws QueryCompilationException
    {
        return visitor.visitAggregateFunctionNode(this);
    }
    
    @Override
    public AstNode copy()
    {
        AggregateFunctionNode copy = new AggregateFunctionNode(m_func_name, this.getLocation());
        
        super.copy(copy);
        
        copy.setSetQuantifier(m_set_quantifier);
        for (ValueExpression expr : getArguments())
        {
            copy.addArgument((ValueExpression) expr.copy());
        }
        
        return copy;
    }
    
}
