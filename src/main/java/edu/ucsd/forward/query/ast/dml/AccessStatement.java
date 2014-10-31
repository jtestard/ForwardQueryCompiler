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
 * The ACCESS statement that navigates into the nested collection.
 * 
 * @author Yupeng
 * 
 */
public class AccessStatement extends AbstractAliasedAstNode implements AliasedDmlStatement
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(AccessStatement.class);
    
    private AttributeReference  m_target;
    
    private ValueExpression     m_condition;
    
    /**
     * Nested DML statement.
     */
    private DmlStatement        m_statement;
    
    /**
     * Default constructor.
     * 
     * @param location
     *            a location.
     */
    public AccessStatement(Location location)
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
     * Gets the nested DML statement.
     * 
     * @return the nested DML statement.
     */
    public DmlStatement getStatement()
    {
        return m_statement;
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
    
    /**
     * Sets the nested DML statement.
     * 
     * @param statement
     *            the nested DML statement.
     */
    public void setStatement(DmlStatement statement)
    {
        assert statement != null;
        if (m_statement != null) removeChild(m_statement);
        m_statement = statement;
        addChild(statement);
    }
    
    @Override
    public void toQueryString(StringBuilder sb, int tabs, DataSource data_source)
    {
        sb.append(this.getIndent(tabs));
        
        sb.append("ACCESS ");
        m_target.toQueryString(sb, 0, data_source);
        sb.append(QueryExpressionPrinter.NL);
        
        if (m_condition != null)
        {
            sb.append(this.getIndent(tabs));
            sb.append("WHERE " + QueryExpressionPrinter.NL);
            m_condition.toQueryString(sb, tabs + 1, data_source);
            sb.append(QueryExpressionPrinter.NL);
        }
        
        m_statement.toQueryString(sb, tabs, data_source);
    }
    
    @Override
    public AstNode copy()
    {
        AccessStatement copy = new AccessStatement(this.getLocation());
        
        super.copy(copy);
        
        copy.setAlias(this.getAlias());
        
        if (this.getTarget() != null)
        {
            copy.setTarget((AttributeReference) this.getTarget().copy());
        }
        
        if (this.getCondition() != null)
        {
            copy.setCondition((ValueExpression) this.getCondition().copy());
        }
        
        if (this.getStatement() != null)
        {
            copy.setStatement((DmlStatement) this.getStatement().copy());
        }
        
        return copy;
    }
    
    @Override
    public Object accept(AstNodeVisitor visitor) throws QueryCompilationException
    {
        return visitor.visitAccessStatement(this);
    }
    
}
