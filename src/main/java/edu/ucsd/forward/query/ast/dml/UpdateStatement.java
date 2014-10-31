/**
 * 
 */
package edu.ucsd.forward.query.ast.dml;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import edu.ucsd.app2you.util.collection.Pair;
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
 * The UPDATE statement.
 * 
 * @author Yupeng
 * 
 */
public class UpdateStatement extends AbstractAliasedAstNode implements AliasedDmlStatement
{
    @SuppressWarnings("unused")
    private static final Logger                             log = Logger.getLogger(UpdateStatement.class);
    
    private AttributeReference                              m_target;
    
    private ValueExpression                                 m_condition;
    
    /**
     * The update list that specifies the attributes and their new values.
     */
    private List<Pair<AttributeReference, ValueExpression>> m_update_list;
    
    /**
     * Default constructor.
     * 
     * @param location
     *            a location.
     */
    public UpdateStatement(Location location)
    {
        super(location);
        
        m_update_list = new ArrayList<Pair<AttributeReference, ValueExpression>>();
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
            if (m_target != null)
            {
                alias = m_target.getPathSteps().get(m_target.getPathSteps().size() - 1);
            }
        }
        
        return alias;
    }
    
    @Override
    public ValueExpression getCondition()
    {
        return m_condition;
    }
    
    /**
     * Sets the attribute reference to the target collection.
     * 
     * @param target_collection
     *            the attribute reference to the target collection.
     */
    public void setTarget(AttributeReference target_collection)
    {
        assert target_collection != null;
        
        if (m_target != null) removeChild(m_target);
        
        m_target = target_collection;
        addChild(m_target);
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
     * Gets the update list.
     * 
     * @return the update list.
     */
    public List<Pair<AttributeReference, ValueExpression>> getUpdateList()
    {
        return m_update_list;
    }
    
    /**
     * Sets the update list.
     * 
     * @param update_list
     *            the update list.
     */
    public void setUpdateList(List<Pair<AttributeReference, ValueExpression>> update_list)
    {
        assert update_list != null;
        List<Pair<AttributeReference, ValueExpression>> update_list_copy;
        update_list_copy = new ArrayList<Pair<AttributeReference, ValueExpression>>(m_update_list);
        
        for (Pair<AttributeReference, ValueExpression> pair : update_list_copy)
        {
            removeUpdateElement(pair);
        }
        for (Pair<AttributeReference, ValueExpression> pair : update_list)
        {
            addUpdateElement(pair.getKey(), pair.getValue());
        }
    }
    
    /**
     * Adds one update element.
     * 
     * @param target
     *            the target of the update.
     * @param source
     *            the source that specifies the new value.
     */
    public void addUpdateElement(AttributeReference target, ValueExpression source)
    {
        assert target != null;
        assert source != null;
        
        addChild(target);
        addChild(source);
        
        m_update_list.add(new Pair<AttributeReference, ValueExpression>(target, source));
    }
    
    /**
     * Removes an update element.
     * 
     * @param element
     *            the update element to be removed.
     */
    public void removeUpdateElement(Pair<AttributeReference, ValueExpression> element)
    {
        assert element != null;
        assert m_update_list.contains(element);
        
        removeChild(element.getKey());
        removeChild(element.getValue());
        m_update_list.remove(element);
    }
    
    @Override
    public void toQueryString(StringBuilder sb, int tabs, DataSource data_source)
    {
        sb.append(this.getIndent(tabs));
        
        if (m_target != null)
        {
            sb.append("UPDATE ");
            m_target.toQueryString(sb, 0, data_source);
            sb.append("SET");
        }
        else
        {
            sb.append("SET ");
        }
        sb.append(QueryExpressionPrinter.NL);
        
        assert (!m_update_list.isEmpty());
        Iterator<Pair<AttributeReference, ValueExpression>> iter = m_update_list.iterator();
        while (iter.hasNext())
        {
            Pair<AttributeReference, ValueExpression> pair = iter.next();
            
            pair.getKey().toQueryString(sb, tabs + 1, data_source);
            sb.append(" = " + QueryExpressionPrinter.NL);
            pair.getValue().toQueryString(sb, tabs + 2, data_source);
            if (iter.hasNext())
            {
                sb.append(", ");
            }
            sb.append(QueryExpressionPrinter.NL);
        }
        
        if (m_condition != null)
        {
            sb.append(this.getIndent(tabs) + "WHERE" + QueryExpressionPrinter.NL);
            m_condition.toQueryString(sb, tabs + 1, data_source);
            sb.append(QueryExpressionPrinter.NL);
        }
    }
    
    @Override
    public AstNode copy()
    {
        UpdateStatement copy = new UpdateStatement(this.getLocation());
        
        super.copy(copy);
        
        if (m_target != null)
        {
            copy.setTarget((AttributeReference) m_target.copy());
            copy.setAlias(this.getAlias());
        }
        
        if (m_condition != null)
        {
            copy.setCondition((ValueExpression) m_condition.copy());
        }
        
        if (m_update_list != null)
        {
            for (Pair<AttributeReference, ValueExpression> pair : m_update_list)
            {
                copy.addUpdateElement((AttributeReference) pair.getKey().copy(), (ValueExpression) pair.getValue().copy());
            }
        }
        
        return copy;
    }
    
    @Override
    public Object accept(AstNodeVisitor visitor) throws QueryCompilationException
    {
        return visitor.visitUpdateStatement(this);
    }
    
}
