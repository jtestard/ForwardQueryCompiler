package edu.ucsd.forward.query.logical;

import java.util.Collections;
import java.util.List;
import java.util.Set;

import edu.ucsd.forward.data.source.DataSourceMetaData;
import edu.ucsd.forward.data.source.DataSourceMetaData.StorageSystem;
import edu.ucsd.forward.data.type.LongType;
import edu.ucsd.forward.data.type.Type;
import edu.ucsd.forward.query.QueryCompilationException;
import edu.ucsd.forward.query.logical.term.ElementVariable;
import edu.ucsd.forward.query.logical.term.Parameter;
import edu.ucsd.forward.query.logical.term.PositionVariable;
import edu.ucsd.forward.query.logical.term.RelativeVariable;
import edu.ucsd.forward.query.logical.term.Variable;
import edu.ucsd.forward.query.logical.visitors.OperatorVisitor;

/**
 * Represents the operator scanning (iterating over) a subquery in the FROM clause.
 * 
 * @author Romain Vernoux
 */
@SuppressWarnings("serial")
public class Subquery extends AbstractUnaryOperator
{
    /**
     * The alias of the subquery.
     */
    private ElementVariable  m_alias_var;
    
    /**
     * The relative variable corresponding to the input order.
     */
    private PositionVariable m_order_var;
    
    /**
     * Constructs an instance of a Subquery operator.
     * 
     * @param alias
     *            the alias of the subquery.
     * @throws QueryCompilationException
     */
    public Subquery(String alias)
    {
        assert (alias != null);
        m_alias_var = new ElementVariable(alias);
    }
    
    /**
     * Default constructor.
     */
    protected Subquery()
    {
        
    }
    
    /**
     * Returns the order variable.
     * 
     * @return the order variable if it is set, <code>null</code> otherwise.
     */
    public PositionVariable getOrderVariable()
    {
        return m_order_var;
    }
    
    /**
     * Sets the order variable.
     * 
     * @param pos_var
     *            the position variable to be set.
     */
    public void setOrderVariable(PositionVariable pos_var)
    {
        assert (pos_var != null);
        m_order_var = pos_var;
    }
    
    /**
     * Returns the element variable corresponding to the alias of the subquery.
     * 
     * @return an element variable corresponding to the alias of the subquery.
     */
    public ElementVariable getAliasVariable()
    {
        return m_alias_var;
    }
    
    @Override
    public List<Variable> getVariablesUsed()
    {
        return Collections.emptyList();
    }
    
    @Override
    public List<Parameter> getFreeParametersUsed()
    {
        return Collections.emptyList();
    }
    
    @Override
    public List<Parameter> getBoundParametersUsed()
    {
        return Collections.emptyList();
    }
    
    @Override
    public void updateOutputInfo() throws QueryCompilationException
    {
        // Output info
        OutputInfo output_info = new OutputInfo();
        
        // Get the child type
        Set<RelativeVariable> child_variables = this.getChild().getOutputInfo().getVariables();
        assert (child_variables.size() == 1);
        RelativeVariable child_var = child_variables.iterator().next();
        Type subquery_type = child_var.getType();
        
        // Compute the output info
        m_alias_var.setType(subquery_type);
        m_alias_var.setBindingIndex(0);
        output_info.add(m_alias_var, child_var);
        
        if (m_order_var != null)
        {
            m_order_var.setType(new LongType());
            m_order_var.setBindingIndex(1);
            output_info.add(m_order_var, child_var);
        }
        
        // Set the actual output info
        this.setOutputInfo(output_info);
    }
    
    @Override
    public boolean isDataSourceCompliant(DataSourceMetaData metadata)
    {
        // If this operator outputs the order of the input subquery, it has to be in memory.
        if (m_order_var != null)
        {
            if (metadata.getStorageSystem() == StorageSystem.INMEMORY)
            {
                return true;
            }
            else
            {
                return false;
            }
        }
        
        return true;
    }
    
    @Override
    public String toExplainString()
    {
        String out = super.toExplainString() + " as " + m_alias_var.getName();
        
        if (m_order_var != null)
        {
            out += " position as " + m_order_var;
        }
        
        return out;
    }
    
    @Override
    public Operator accept(OperatorVisitor visitor)
    {
        return visitor.visitSubquery(this);
    }
    
    @Override
    public Operator copy()
    {
        Subquery copy = new Subquery(m_alias_var.getName());
        
        if (m_order_var != null)
        {
            copy.setOrderVariable((PositionVariable) m_order_var.copy());
        }
        
        super.copy(copy);
        
        return copy;
    }
    
    @Override
    public Operator copyWithoutType()
    {
        Subquery copy = new Subquery(m_alias_var.getName());
        
        if (m_order_var != null)
        {
            copy.setOrderVariable(m_order_var);
        }
        
        super.copy(copy);
        
        return copy;
    }
}
