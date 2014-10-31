/**
 * 
 */
package edu.ucsd.forward.query.ast.dml;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.source.DataSource;
import edu.ucsd.forward.exception.Location;
import edu.ucsd.forward.query.QueryCompilationException;
import edu.ucsd.forward.query.ast.AbstractQueryConstruct;
import edu.ucsd.forward.query.ast.AstNode;
import edu.ucsd.forward.query.ast.AttributeReference;
import edu.ucsd.forward.query.ast.ValueExpression;
import edu.ucsd.forward.query.ast.visitors.AstNodeVisitor;
import edu.ucsd.forward.query.explain.QueryExpressionPrinter;

/**
 * The INSERT statement.
 * 
 * @author Yupeng
 * 
 */
public class InsertStatement extends AbstractQueryConstruct implements DmlStatement
{
    @SuppressWarnings("unused")
    private static final Logger   log = Logger.getLogger(InsertStatement.class);
    
    /**
     * The fields of a composite column where inserting into only and leaves the other fields null. If the list is null, then no
     * column is specified.
     * 
     */
    private List<String>          m_target_attrs;
    
    private AttributeReference    m_target_collection;
    
    /**
     * The value expression that specifies the tuple(s) to be inserted.
     */
    private ValueExpression       m_value_expression;
    
    /**
     * The value expressions that specify the values of each column.
     */
    private List<ValueExpression> m_expressions;
    
    /**
     * Indicates if all columns will be filled with their default values.
     */
    private boolean               m_is_default_values;
    
    /**
     * Default constructor.
     * 
     * @param location
     *            a location.
     */
    public InsertStatement(Location location)
    {
        super(location);
        
        m_is_default_values = false;
        m_target_attrs = new ArrayList<String>();
        m_expressions = new ArrayList<ValueExpression>();
    }
    
    /**
     * Constructs with the target collection.
     * 
     * @param target_collection
     *            the target collection into where the tuples are to be inserted.
     * @param location
     *            a location.
     */
    public InsertStatement(AttributeReference target_collection, Location location)
    {
        this(location);
        setTargetCollection(target_collection);
    }
    
    @Override
    public AttributeReference getTarget()
    {
        return m_target_collection;
    }
    
    /**
     * Gets the target attributes.
     * 
     * @return the target attributes.
     */
    public List<String> getTargetAttributes()
    {
        return Collections.unmodifiableList(m_target_attrs);
    }
    
    /**
     * Sets the the target attributes.
     * 
     * @param target_attrs
     *            the list of target attributes.
     */
    public void setTargetAttributes(List<String> target_attrs)
    {
        assert (target_attrs != null);
        assert (!target_attrs.isEmpty());
        
        m_target_attrs = target_attrs;
    }
    
    /**
     * Sets the value expression.
     * 
     * @param value_expression
     *            the value expression.
     */
    public void setValueExpression(ValueExpression value_expression)
    {
        assert value_expression != null;
        
        if (m_value_expression != null) removeChild(m_value_expression);
        
        m_value_expression = value_expression;
        
        addChild(m_value_expression);
    }
    
    /**
     * Sets the value constructors.
     * 
     * @param expressions
     *            the value expressions.
     */
    public void setValueConstructors(List<ValueExpression> expressions)
    {
        assert (expressions != null);
        assert (!expressions.isEmpty());
        
        // Remove all the old expressions.
        for (ValueExpression expression : m_expressions)
        {
            removeValueExpression(expression);
        }
        
        // Add new expressions
        for (ValueExpression expression : expressions)
        {
            addValueExpression(expression);
        }
    }
    
    /**
     * Adds one value expression.
     * 
     * @param expression
     *            the value expression.
     */
    public void addValueExpression(ValueExpression expression)
    {
        assert expression != null;
        
        m_expressions.add(expression);
        addChild(expression);
    }
    
    /**
     * Removes one value expression.
     * 
     * @param expression
     *            the value expression to be removed.
     */
    public void removeValueExpression(ValueExpression expression)
    {
        assert expression != null;
        assert m_expressions.contains(expression);
        
        removeChild(expression);
        m_expressions.remove(expression);
    }
    
    /**
     * Gets the value constructors.
     * 
     * @return the value constructors.
     */
    public List<ValueExpression> getValueConstructors()
    {
        return m_expressions;
    }
    
    /**
     * Sets the flag that indicates if all columns will be filled with their default values.
     * 
     * @param is_default_value
     *            the flag value
     */
    public void setDefaultValues(boolean is_default_value)
    {
        m_is_default_values = is_default_value;
    }
    
    /**
     * Sets the attribute reference to the target collection.
     * 
     * @param target_collection
     *            the attribute reference to the target collection.
     */
    public void setTargetCollection(AttributeReference target_collection)
    {
        assert target_collection != null;
        
        if (m_target_collection != null) removeChild(m_target_collection);
        
        m_target_collection = target_collection;
        addChild(m_target_collection);
    }
    
    /**
     * Gets the value expression.
     * 
     * @return the value expression.
     */
    public ValueExpression getValueExpression()
    {
        return m_value_expression;
    }
    
    /**
     * Checks if all columns will be filled with their default values.
     * 
     * @return <code>true</code> if all columns will be filled with their default values; otherwise <code>false</code>.
     */
    public boolean isDefaultValues()
    {
        return m_is_default_values;
    }
    
    @Override
    public void toQueryString(StringBuilder sb, int tabs, DataSource data_source)
    {
        sb.append(this.getIndent(tabs));
        
        sb.append("INSERT INTO ");
        m_target_collection.toQueryString(sb, 0, data_source);
        if (!m_target_attrs.isEmpty())
        {
            sb.append(QueryExpressionPrinter.LEFT_PAREN);
            for (String column : m_target_attrs)
            {
                sb.append(column + ", ");
            }
            sb.delete(sb.length() - 2, sb.length());
            sb.append(QueryExpressionPrinter.RIGHT_PAREN);
        }
        sb.append(QueryExpressionPrinter.NL);
        
        if (m_is_default_values)
        {
            sb.append(this.getIndent(tabs + 1) + "DEFAULT VALUES" + QueryExpressionPrinter.NL);
        }
        
        if (m_value_expression != null)
        {
            m_value_expression.toQueryString(sb, tabs + 1, data_source);
        }
        else
        {
            sb.append(this.getIndent(tabs + 1) + "VALUES" + QueryExpressionPrinter.LEFT_PAREN + QueryExpressionPrinter.NL);
            Iterator<ValueExpression> iter = m_expressions.iterator();
            while (iter.hasNext())
            {
                ValueExpression expression = iter.next();
                
                expression.toQueryString(sb, tabs + 2, data_source);
                if (iter.hasNext())
                {
                    sb.append(", ");
                }
                sb.append(QueryExpressionPrinter.NL);
            }
            sb.delete(sb.length() - 2, sb.length());
            sb.append(QueryExpressionPrinter.RIGHT_PAREN);
            return;
        }
        
        sb.append(QueryExpressionPrinter.NL);
    }
    
    @Override
    public AstNode copy()
    {
        InsertStatement copy = new InsertStatement(this.getLocation());
        
        super.copy(copy);
        
        if (!m_target_attrs.isEmpty())
        {
            copy.setTargetAttributes(new ArrayList<String>(m_target_attrs));
        }
        
        if (m_target_collection != null)
        {
            copy.setTargetCollection((AttributeReference) m_target_collection.copy());
        }
        
        if (m_value_expression != null)
        {
            copy.setValueExpression((ValueExpression) m_value_expression.copy());
        }
        
        if (!m_expressions.isEmpty())
        {
            ArrayList<ValueExpression> expressions_copy = new ArrayList<ValueExpression>();
            for (ValueExpression expr : m_expressions)
                expressions_copy.add((ValueExpression) expr.copy());
            copy.setValueConstructors(expressions_copy);
        }
        
        copy.setDefaultValues(m_is_default_values);
        
        return copy;
    }
    
    @Override
    public Object accept(AstNodeVisitor visitor) throws QueryCompilationException
    {
        return visitor.visitInsertStatement(this);
    }
    
}
