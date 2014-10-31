package edu.ucsd.forward.query.ast;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import edu.ucsd.app2you.util.identity.DeepEquality;
import edu.ucsd.app2you.util.identity.EqualUtil;
import edu.ucsd.app2you.util.identity.HashCodeUtil;
import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.source.DataSource;
import edu.ucsd.forward.exception.Location;
import edu.ucsd.forward.query.QueryCompilationException;
import edu.ucsd.forward.query.ast.function.FunctionNode;
import edu.ucsd.forward.query.ast.visitors.AstNodeVisitor;

/**
 * The attribute reference is a list of identifiers, either absolute or relative.
 * 
 * @author Yupeng
 * @author Michalis Petropoulos
 * 
 */
@SuppressWarnings("serial")
public class AttributeReference extends AbstractValueExpression implements DeepEquality
{
    @SuppressWarnings("unused")
    private static final Logger log            = Logger.getLogger(AttributeReference.class);
    
    public static final String  PATH_SEPARATOR = ".";
    
    /**
     * In case the attribute reference starts with a function node.
     */
    private FunctionNode        m_func_node    = null;
    
    private List<String>        m_path_steps;
    
    /**
     * The constructor with a list of path steps.
     * 
     * @param steps
     *            the path steps.
     * @param location
     *            a location.
     */
    public AttributeReference(List<String> steps, Location location)
    {
        super(location);
        
        m_path_steps = new ArrayList<String>(steps);
    }
    
    /**
     * Gets the length of the attribute reference.
     * 
     * @return the length of the attribute reference.
     */
    public int getLength()
    {
        return m_path_steps.size();
    }
    
    /**
     * Gets the path steps.
     * 
     * @return the path steps.
     */
    public List<String> getPathSteps()
    {
        return Collections.unmodifiableList(m_path_steps);
    }
    
    /**
     * Sets the path steps.
     * 
     * @param path_steps
     *            the path steps to set.
     */
    public void setPathSteps(List<String> path_steps)
    {
        assert (path_steps != null);
        assert (!path_steps.isEmpty());
        
        m_path_steps = path_steps;
    }
    
    /**
     * Gets the starting function node.
     * 
     * @return the starting function node.
     */
    public FunctionNode getStartingFunctionNode()
    {
        return m_func_node;
    }
    
    /**
     * Sets the starting function node.
     * 
     * @param func_node
     *            the starting function node.
     */
    public void setStartingFunctionNode(FunctionNode func_node)
    {
        assert (func_node != null);
        
        m_func_node = func_node;
    }
    
    /**
     * Returns whether this attribute reference is a suffix of the input attribute reference.
     * 
     * @param attr_ref
     *            the input attribute reference.
     * @return <code>true</code> if this attribute reference is a suffix of the input attribute reference; <code>false</code>
     *         otherwise.
     */
    public boolean isSuffix(AttributeReference attr_ref)
    {
        assert (attr_ref != null);
        
        // Not a suffix if this attribute reference is longer
        if (this.getLength() > attr_ref.getLength())
        {
            return false;
        }
        
        List<String> path_steps = attr_ref.getPathSteps();
        int long_length = attr_ref.getLength();
        int short_length = this.getLength();
        for (int i = 1; i <= short_length; i++)
        {
            // Not a suffix if a step does not match
            if (!m_path_steps.get(short_length - i).equals(path_steps.get(long_length - i)))
            {
                return false;
            }
        }
        
        return true;
    }
    
    /**
     * Returns whether this attribute reference is a prefix of the input attribute reference.
     * 
     * @param attr_ref
     *            the input attribute reference.
     * @return <code>true</code> if this attribute reference is a prefix of the input attribute reference; <code>false</code>
     *         otherwise.
     */
    public boolean isPrefix(AttributeReference attr_ref)
    {
        assert (attr_ref != null);
        
        // Not a prefix if this attribute reference is longer
        if (this.getLength() > attr_ref.getLength())
        {
            return false;
        }
        
        List<String> path_steps = attr_ref.getPathSteps();
        int short_length = this.getLength();
        for (int i = 0; i < short_length; i++)
        {
            // Not a prefix if a step does not match
            if (!m_path_steps.get(i).equals(path_steps.get(i)))
            {
                return false;
            }
        }
        
        return true;
    }
    
    @Override
    public void toQueryString(StringBuilder sb, int tabs, DataSource data_source)
    {
        sb.append(this.getIndent(tabs));
        
        for (String step : m_path_steps)
        {
            sb.append(step + PATH_SEPARATOR);
        }
        sb.delete(sb.length() - 1, sb.length());
        
    }
    
    @Override
    public String toExplainString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append(super.toExplainString() + " -> ");
        this.toQueryString(sb, 0, null);
        return sb.toString();
    }
    
    @Override
    public Object accept(AstNodeVisitor visitor) throws QueryCompilationException
    {
        return visitor.visitAttributeReference(this);
    }
    
    @Override
    public AstNode copy()
    {
        AttributeReference copy = new AttributeReference(new ArrayList<String>(m_path_steps), this.getLocation());
        
        if (m_func_node != null)
        {
            copy.setStartingFunctionNode((FunctionNode) m_func_node.copy());
        }
        
        super.copy(copy);
        
        return copy;
        
    }
    
    @Override
    public Object[] getDeepEqualityObjects()
    {
        return new Object[] { m_path_steps };
    }
    
    @Override
    public boolean equals(Object x)
    {
        return EqualUtil.equalsByDeepEquality(this, x);
    }
    
    @Override
    public int hashCode()
    {
        return HashCodeUtil.hashCodeByDeepEquality(this);
    }
}
