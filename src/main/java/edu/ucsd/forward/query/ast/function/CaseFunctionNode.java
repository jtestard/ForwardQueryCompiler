package edu.ucsd.forward.query.ast.function;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.source.DataSource;
import edu.ucsd.forward.exception.Location;
import edu.ucsd.forward.query.QueryCompilationException;
import edu.ucsd.forward.query.ast.AstNode;
import edu.ucsd.forward.query.ast.ValueExpression;
import edu.ucsd.forward.query.ast.visitors.AstNodeVisitor;
import edu.ucsd.forward.query.explain.QueryExpressionPrinter;
import edu.ucsd.forward.query.function.conditional.CaseFunction;

/**
 * A case function call.
 * 
 * @author Michalis Petropoulos
 * 
 */
@SuppressWarnings("serial")
public class CaseFunctionNode extends AbstractFunctionNode
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(CaseFunctionNode.class);
    
    /**
     * Default constructor used by the query parser.
     * 
     * @param location
     *            a location.
     */
    public CaseFunctionNode(Location location)
    {
        this(Collections.<ValueExpression> emptyList(), location);
    }
    
    /**
     * Constructs the case function call with arguments.
     * 
     * @param arguments
     *            arguments
     * @param location
     *            a location.
     */
    public CaseFunctionNode(List<ValueExpression> arguments, Location location)
    {
        super(CaseFunction.NAME, location);
        
        for (ValueExpression argument : arguments)
        {
            addArgument(argument);
        }
    }
    
    @Override
    public void toQueryString(StringBuilder sb, int tabs, DataSource data_source)
    {
        sb.append(this.getIndent(tabs) + "CASE" + QueryExpressionPrinter.NL);
        
        Iterator<ValueExpression> iter_args = this.getArguments().iterator();
        while (iter_args.hasNext())
        {
            ValueExpression when = iter_args.next();
            sb.append(this.getIndent(tabs + 1) + "WHEN ");
            when.toQueryString(sb, 0, data_source);
            sb.append(QueryExpressionPrinter.NL);
            ValueExpression then = iter_args.next();
            sb.append(this.getIndent(tabs + 1) + "THEN ");
            then.toQueryString(sb, 0, data_source);
            sb.append(QueryExpressionPrinter.NL);
        }
        sb.append(this.getIndent(tabs) + "END");
        
    }
    
    @Override
    public String toExplainString()
    {
        StringBuilder sb = new StringBuilder();
        
        sb.append("CASE");
        
        Iterator<ValueExpression> iter_args = this.getArguments().iterator();
        while (iter_args.hasNext())
        {
            ValueExpression when = iter_args.next();
            sb.append(" WHEN ");
            sb.append(when.toExplainString());
            ValueExpression then = iter_args.next();
            sb.append(" THEN ");
            sb.append(then.toExplainString());
        }
        sb.append(" END");
        
        
        sb.append(" -> " + this.getFunctionName());
        
        return sb.toString();
    }
    
    @Override
    public Object accept(AstNodeVisitor visitor) throws QueryCompilationException
    {
        return visitor.visitCaseFunctionNode(this);
    }
    
    @Override
    public AstNode copy()
    {
        CaseFunctionNode copy = new CaseFunctionNode(this.getLocation());
        
        super.copy(copy);
        
        for (ValueExpression expr : this.getArguments())
        {
            copy.addArgument((ValueExpression) expr.copy());
        }
        
        return copy;
    }
    
}
