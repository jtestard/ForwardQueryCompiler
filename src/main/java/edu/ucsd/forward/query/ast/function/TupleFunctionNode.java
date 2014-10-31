package edu.ucsd.forward.query.ast.function;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.source.DataSource;
import edu.ucsd.forward.exception.Location;
import edu.ucsd.forward.query.QueryCompilationException;
import edu.ucsd.forward.query.ast.AstNode;
import edu.ucsd.forward.query.ast.QueryConstruct;
import edu.ucsd.forward.query.ast.TupleAllItem;
import edu.ucsd.forward.query.ast.TupleItem;
import edu.ucsd.forward.query.ast.ValueExpression;
import edu.ucsd.forward.query.ast.visitors.AstNodeVisitor;
import edu.ucsd.forward.query.explain.QueryExpressionPrinter;
import edu.ucsd.forward.query.function.tuple.TupleFunction;

/**
 * A tuple function call, which has a list of aliased arguments. The arguments are child AST nodes of the function call node.
 * 
 * @author Michalis Petropoulos
 * 
 */
public class TupleFunctionNode extends AbstractFunctionNode
{
    private static final long   serialVersionUID = 1L;
    
    @SuppressWarnings("unused")
    private static final Logger log              = Logger.getLogger(TupleFunctionNode.class);
    
    private boolean             m_inline         = false;
    
    /**
     * Checks if the tuple node is set as inline.
     * 
     * @return whether the tuple node is set as inline.
     */
    public boolean isInline()
    {
        return m_inline;
    }
    
    /**
     * Sets the tuple node as inline.
     * 
     * @param inline
     *            inline value.
     */
    public void setInline(boolean inline)
    {
        m_inline = inline;
    }
    
    /**
     * Constructs the function node.
     * 
     * @param location
     *            a location.
     */
    public TupleFunctionNode(Location location)
    {
        super(TupleFunction.NAME, location);
    }
    
    /**
     * Adds one aliased argument.
     * 
     * @param item
     *            the tuple item to be added.
     */
    public void addArgument(QueryConstruct item)
    {
        assert (item != null);
        assert item instanceof TupleItem || item instanceof TupleAllItem;
        this.addChild(item);
    }
    
    /**
     * Adds one aliased argument at the specified position in this list (optional operation). Shifts the argument currently at that
     * position (if any) and any subsequent elements to the right (adds one to their indices).
     * 
     * @param index
     *            index at which the specified argument is to be inserted.
     * @param item
     *            the tuple item to be added.
     */
    public void addArgument(int index, TupleItem item)
    {
        assert (item != null);
        this.addChild(index, item);
    }
    
    @Override
    public void removeArguments()
    {
        for (QueryConstruct item : this.getTupleItems())
        {
            this.removeChild(item);
        }
    }
    
    /**
     * Removes one aliased argument from the function, and sets the parent of the argument to be null.
     * 
     * @param item
     *            the tuple item to be removed.
     */
    public void removeArgument(TupleItem item)
    {
        assert (item != null);
        this.removeChild(item);
    }
    
    @Override
    public List<ValueExpression> getArguments()
    {
        List<ValueExpression> children = new ArrayList<ValueExpression>();
        for (AstNode arg : this.getChildren())
        {
            children.add(((TupleItem) arg).getExpression());
        }
        
        return children;
    }
    
    /**
     * Gets the function call arguments.
     * 
     * @return the function call arguments.
     */
    public List<QueryConstruct> getTupleItems()
    {
        List<QueryConstruct> children = new ArrayList<QueryConstruct>();
        for (AstNode arg : this.getChildren())
        {
            children.add((QueryConstruct) arg);
        }
        
        return children;
    }
    
    @Override
    public void toQueryString(StringBuilder sb, int tabs, DataSource data_source)
    {
        sb.append(this.getIndent(tabs));
        
        if (isInline())
        {
            sb.append("INLINE ");
        }
        
        sb.append(TupleFunction.NAME + QueryExpressionPrinter.LEFT_PAREN + QueryExpressionPrinter.NL);
        
        Iterator<QueryConstruct> iter = this.getTupleItems().iterator();
        while (iter.hasNext())
        {
            QueryConstruct argument = iter.next();
            argument.toQueryString(sb, tabs + 1, data_source);
            if (iter.hasNext())
            {
                sb.append(", ");
            }
            sb.append(QueryExpressionPrinter.NL);
        }
        sb.append(this.getIndent(tabs) + QueryExpressionPrinter.RIGHT_PAREN);
        
    }
    
    @Override
    public String toExplainString()
    {
        String out = super.toExplainString();
        out += " -> " + this.getFunctionName();
        
        return out;
    }
    
    @Override
    public Object accept(AstNodeVisitor visitor) throws QueryCompilationException
    {
        return visitor.visitTupleFunctionNode(this);
    }
    
    @Override
    public AstNode copy()
    {
        TupleFunctionNode copy = new TupleFunctionNode(this.getLocation());
        
        super.copy(copy);
        copy.setInline(m_inline);
        
        for (QueryConstruct expr : getTupleItems())
        {
            copy.addArgument((QueryConstruct) expr.copy());
        }
        
        return copy;
    }
    
}
