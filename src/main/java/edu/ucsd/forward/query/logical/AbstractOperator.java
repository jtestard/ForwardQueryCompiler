package edu.ucsd.forward.query.logical;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import edu.ucsd.forward.data.source.DataSource;
import edu.ucsd.forward.data.source.DataSourceException;
import edu.ucsd.forward.data.source.DataSourceMetaData;
import edu.ucsd.forward.data.source.DataSourceMetaData.Site;
import edu.ucsd.forward.data.source.DataSourceMetaData.StorageSystem;
import edu.ucsd.forward.query.QueryCompilationException;
import edu.ucsd.forward.query.QueryProcessorFactory;
import edu.ucsd.forward.query.logical.dml.DmlOperator;
import edu.ucsd.forward.query.logical.plan.LogicalPlan;
import edu.ucsd.forward.query.logical.term.Parameter;
import edu.ucsd.forward.query.logical.term.RelativeVariable;
import edu.ucsd.forward.query.logical.term.Variable;
import edu.ucsd.forward.util.tree.AbstractTreeNode2;

/**
 * Represents an abstract logical operator in a logical query plan.
 * 
 * @author Michalis Petropoulos
 */

@SuppressWarnings("serial")
public abstract class AbstractOperator extends AbstractTreeNode2<Operator, Operator, Operator, CardinalityEstimate.Size, Operator>
        implements Operator
{
    /**
     * The output info of the operator.
     */
    private OutputInfo m_output_info;
    
    /**
     * The name of the execution data source of the operator.
     */
    private String     m_exec_data_source;
    
    private String     m_impl_method = null;
    
    /**
     * @return the impl_method
     */
    public String getImplementationMethod()
    {
        return m_impl_method;
    }
    
    /**
     * @param implMethod
     *            the impl_method to set
     */
    public void setImplementationMethod(String implMethod)
    {
        m_impl_method = implMethod;
    }
    
    /**
     * Constructs an abstract operator.
     */
    protected AbstractOperator()
    {
        this.setCardinalityEstimate(Size.UNKNOWN);
    }
    
    @Override
    public String getName()
    {
        // return this.getClass().getSimpleName();
        String class_name = this.getClass().getName();
        
        return class_name.substring(class_name.lastIndexOf('.') + 1, class_name.length());
    }
    
    @Override
    public void addChild(Operator child)
    {
        super.addChild(child.getCardinalityEstimate(), child);
    }
    
    @Override
    public void addChild(int index, Operator child)
    {
        super.addChild(child.getCardinalityEstimate(), index, child);
    }
    
    @Override
    public void removeChild(Operator child)
    {
        super.removeChild(child);
    }
    
    @Override
    public OutputInfo getOutputInfo()
    {
        return m_output_info;
    }
    
    /**
     * Sets the output info of the operator.
     * 
     * @param output_info
     *            the output info of the operator.
     * @throws QueryCompilationException
     *             if there is a query compilation error.
     */
    protected void setOutputInfo(OutputInfo output_info) throws QueryCompilationException
    {
        m_output_info = output_info;
        
        // Set the binding index of the variables used
        for (Variable var : this.getVariablesUsed())
        {
            if (var instanceof RelativeVariable)
            {
                int index = this.getInputVariables().indexOf(var);
                var.setBindingIndex(index);
            }
        }
        
        // Set the binding index of the bound parameters used
        for (Parameter param : this.getBoundParametersUsed())
        {
            Variable var = param.getVariable();
            var.setBindingIndex(this.getInputVariables().indexOf(var));
        }
        
        for (LogicalPlan logical_plan : this.getLogicalPlansUsed())
        {
            // Make sure the plan is marked as nested.
            logical_plan.setNested(true);
        }
    }
    
    @Override
    public List<RelativeVariable> getInputVariables()
    {
        List<RelativeVariable> in_variables = new ArrayList<RelativeVariable>();
        for (Operator operator : this.getChildren())
        {
            in_variables.addAll(operator.getOutputInfo().getVariables());
        }
        
        return in_variables;
    }
    
    @Override
    public List<LogicalPlan> getLogicalPlansUsed()
    {
        return Collections.<LogicalPlan> emptyList();
    }
    
    @Override
    public Size getCardinalityEstimate()
    {
        return this.getEdgeLabel();
    }
    
    @Override
    public void setCardinalityEstimate(Size size)
    {
        this.setEdgeLabel(size);
    }
    
    @Override
    public boolean isDataSourceCompliant(DataSourceMetaData metadata)
    {
        switch (metadata.getStorageSystem())
        {
            case INDEXEDDB:
                // Only DML operator, scan and index scan are compliant with indexedDB operator.
                if (!(this instanceof DmlOperator) && !(this instanceof Scan) && !(this instanceof IndexScan))
                {
                    return false;
                }
        }
        
        switch (metadata.getDataModel())
        {
            case SQLPLUSPLUS:
                // Always compliant with SQL++ data sources
                return true;
            case RELATIONAL:
                // Not always compliant with relational data sources
                // Check that the output schemas of the child operators are relational.
                for (Operator child : this.getChildren())
                {
                    for (RelativeVariable var : child.getOutputInfo().getVariables())
                    {
                        if (!var.getType().isDataSourceCompliant(metadata)) return false;
                    }
                }
                
                // Check that the output schema of the operator is relational.
                for (RelativeVariable var : this.getOutputInfo().getVariables())
                {
                    if (!var.getType().isDataSourceCompliant(metadata)) return false;
                }
                
                // The tuple type doesn't need to have at least one attribute.
                // The empty tuple type is considered relational.
                
                return true;
            default:
                throw new AssertionError();
        }
    }
    
    @Override
    public String getExecutionDataSourceName()
    {
        return m_exec_data_source;
    }
    
    @Override
    public void setExecutionDataSourceName(String data_source_name)
    {
        assert (data_source_name != null);
        
        // Retrieve the data source metadata
        DataSourceMetaData metadata = null;
        try
        {
            metadata = QueryProcessorFactory.getInstance().getDataSourceMetaData(data_source_name);
        }
        catch (DataSourceException e)
        {
            // This should never happen
            throw new AssertionError(e);
        }
        
        // If the storage system of the data source is INMEMORY, then set the execution data source to be the MEDIATOR.
        // This is very important in order to avoid the eager evaluation of query plans in order to copy their result to other
        // INMEMORY data sources.
        if (metadata.getStorageSystem() == StorageSystem.INMEMORY)
        {
            if (metadata.getSite() == Site.CLIENT)
            {
                m_exec_data_source = DataSource.CLIENT;
            }
            else
            {
                m_exec_data_source = DataSource.MEDIATOR;
            }
        }
        else
        {
            m_exec_data_source = data_source_name;
        }
    }
    
    @Override
    public String toExplainString()
    {
        String str = "";
        
        if (getExecutionDataSourceName() != null)
        {
            if (!str.isEmpty()) str += " ";
            str += this.getExecutionDataSourceName();
        }
        
        if (!str.isEmpty()) str = "(" + str + ")";
        
        return getName() + str;
    }
    
    /**
     * Get all descendants and self of type InnerJoin.
     * 
     * @return all the descendants and self of type InnerJoin.
     */
    public Collection<InnerJoin> getInnerJoinDescendantsAndSelf()
    {
        List<InnerJoin> list = new ArrayList<InnerJoin>();
        for (Operator node : getDescendantsAndSelf())
        {
            if (node instanceof InnerJoin) list.add((InnerJoin) node);
        }
        return list;
    }
    
    @Override
    public String toString()
    {
        return toExplainString();
    }
    
    /**
     * Copies common attributes to an Operator copy.
     * 
     * @param copy
     *            an Operator copy.
     */
    protected void copy(Operator copy)
    {
        if (this.getExecutionDataSourceName() != null)
        {
            copy.setExecutionDataSourceName(this.getExecutionDataSourceName());
        }
        copy.setCardinalityEstimate(this.getCardinalityEstimate());
    }
    

    @Override
    public Operator copyWithoutType()
    {
        throw new UnsupportedOperationException();
    }
}
