/**
 * 
 */
package edu.ucsd.forward.query.ast.dml;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.source.DataSource;
import edu.ucsd.forward.exception.Location;
import edu.ucsd.forward.query.QueryCompilationException;
import edu.ucsd.forward.query.ast.AbstractAliasedAstNode;
import edu.ucsd.forward.query.ast.AstNode;
import edu.ucsd.forward.query.ast.AttributeReference;
import edu.ucsd.forward.query.ast.ValueExpression;
import edu.ucsd.forward.query.ast.visitors.AstNodeVisitor;
import edu.ucsd.forward.query.explain.QueryExpressionPrinter;

/**
 * The DELETE statement that deletes tuples from collection.
 * 
 * @author Yupeng
 * 
 */
public class DeleteStatement extends AbstractAliasedAstNode implements AliasedDmlStatement
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(DeleteStatement.class);
    
    private AttributeReference  m_target;
    
    private ValueExpression     m_condition;
    
    /**
     * Default constructor.
     * 
     * @param location
     *            a location.
     */
    public DeleteStatement(Location location)
    {
        super(location);
    }
    
    @Override
    public AttributeReference getTarget()
    {
        return m_target;
    }
    
    @Override
    public String getAlias()
    {
        String alias = super.getAlias();
        
        // If the alias is missing, use the last step of the target attribute reference
        if (alias == null)
        {
            alias = m_target.getPathSteps().get(m_target.getPathSteps().size() - 1);
        }
        
        return alias;
    }
    
    @Override
    public ValueExpression getCondition()
    {
        return m_condition;
    }
    
    /**
     * Sets the attribute reference to the target.
     * 
     * @param target
     *            the attribute reference to the target.
     */
    public void setTarget(AttributeReference target)
    {
        assert target != null;
        
        if (m_target != null) removeChild(m_target);
        
        m_target = target;
        addChild(target);
    }
    
    /**
     * Sets the search condition.
     * 
     * @param condition
     *            the search condition.
     */
    public void setCondition(ValueExpression condition)
    {
        assert condition != null;
        if (m_condition != null) removeChild(m_condition);
        m_condition = condition;
        addChild(condition);
    }
    
    @Override
    public void toQueryString(StringBuilder sb, int tabs, DataSource data_source)
    {
        sb.append(this.getIndent(tabs));
        
        sb.append("DELETE FROM ");
        m_target.toQueryString(sb, 0, data_source);
        sb.append(" AS " + this.getAlias() + QueryExpressionPrinter.NL);
        
        if (m_condition != null)
        {
            sb.append(this.getIndent(tabs));
            sb.append("WHERE " + QueryExpressionPrinter.NL);
            m_condition.toQueryString(sb, tabs + 1, data_source);
            sb.append(QueryExpressionPrinter.NL);
        }
        
    }
    
    @Override
    public AstNode copy()
    {
        DeleteStatement copy = new DeleteStatement(this.getLocation());
        
        super.copy(copy);
        
        copy.setAlias(this.getAlias());
        
        if (m_target != null)
        {
            copy.setTarget((AttributeReference) m_target.copy());
        }
        
        if (m_condition != null)
        {
            copy.setCondition((ValueExpression) m_condition.copy());
        }
        
        return copy;
    }
    
    @Override
    public Object accept(AstNodeVisitor visitor) throws QueryCompilationException
    {
        return visitor.visitDeleteStatement(this);
    }
    
}
