package edu.ucsd.forward.query.ast.function;

import java.util.ArrayList;
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
import edu.ucsd.forward.query.function.collection.CollectionFunction;

/**
 * A collection function call, which has a list of tuples. The tuples are child AST nodes of the function call node.
 * 
 * @author Romain Vernoux
 * 
 */
public class CollectionFunctionNode extends AbstractFunctionNode
{
    private static final long   serialVersionUID = 1L;
    
    @SuppressWarnings("unused")
    private static final Logger log              = Logger.getLogger(CollectionFunctionNode.class);
    
    private boolean             m_ordered        = false;
    
    /**
     * Constructs the function node.
     * 
     * @param location
     *            a location.
     */
    public CollectionFunctionNode(Location location)
    {
        super(CollectionFunction.NAME, location);
    }
    
    /**
     * Adds one argument.
     * 
     * @param item
     *            the tuple to be added.
     */
    public void addArgument(ValueExpression item)
    {
        assert (item != null);
        this.addChild(item);
    }
    
    @Override
    public void removeArguments()
    {
        for (ValueExpression item : this.getElements())
        {
            this.removeChild(item);
        }
    }
    
    /**
     * Removes one aliased argument from the function, and sets the parent of the argument to be null.
     * 
     * @param element
     *            the element node to be removed.
     */
    public void removeArgument(ValueExpression element)
    {
        assert (element != null);
        this.removeChild(element);
    }
    
    @Override
    public List<ValueExpression> getArguments()
    {
        List<ValueExpression> children = new ArrayList<ValueExpression>();
        for (AstNode arg : this.getChildren())
        {
            assert(arg instanceof ValueExpression);
            children.add((ValueExpression) arg);
        }
        
        return children;
    }
    
    /**
     * Gets the elements of the collection.
     * 
     * @return the elements of the collection.
     */
    public List<ValueExpression> getElements()
    {
        return getArguments();
    }
    
    /**
     * Gets the flag telling if this node corresponds to a BAG call or a LIST call.
     * 
     * @return <code>true</code> if this node corresponds to a LIST call, <code>false</code> otherwise.
     */
    public boolean isOrdered()
    {
        return m_ordered;
    }
    
    /**
     * Sets the flag telling if this node corresponds to a BAG call or a LIST call.
     * 
     * @param flag
     *            should be if this node corresponds to a LIST call, <code>false</code> otherwise.
     */
    public void setOrdered(boolean flag)
    {
        m_ordered = flag;
    }
    
    @Override
    public void toQueryString(StringBuilder sb, int tabs, DataSource data_source)
    {
        sb.append(this.getIndent(tabs));
        
        sb.append(CollectionFunction.NAME + QueryExpressionPrinter.LEFT_PAREN + QueryExpressionPrinter.NL);
        
        Iterator<ValueExpression> iter = this.getElements().iterator();
        while (iter.hasNext())
        {
            ValueExpression argument = iter.next();
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
        return visitor.visitCollectionFunctionNode(this);
    }
    
    @Override
    public AstNode copy()
    {
        CollectionFunctionNode copy = new CollectionFunctionNode(this.getLocation());
        
        super.copy(copy);
        
        for (ValueExpression expr : getElements())
        {
            copy.addArgument((ValueExpression) expr.copy());
        }
        
        copy.setOrdered(m_ordered);
        
        return copy;
    }
    
}
