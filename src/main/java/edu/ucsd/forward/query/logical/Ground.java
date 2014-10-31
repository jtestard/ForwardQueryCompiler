package edu.ucsd.forward.query.logical;

import java.util.Collections;
import java.util.List;

import edu.ucsd.forward.data.source.DataSourceMetaData;
import edu.ucsd.forward.query.QueryCompilationException;
import edu.ucsd.forward.query.logical.term.Parameter;
import edu.ucsd.forward.query.logical.term.Variable;
import edu.ucsd.forward.query.logical.visitors.OperatorVisitor;

/**
 * Represents the operator scanning a data object that has a single empty binding.
 * 
 * @author Michalis Petropoulos
 * 
 */
@SuppressWarnings("serial")
public class Ground extends AbstractOperator
{
    /**
     * Constructs an instance of a data object operator.
     */
    public Ground()
    {
        super();
    }
    
    @Override
    public List<Variable> getVariablesUsed()
    {
        return Collections.emptyList();
    }
    
    @Override
    public List<Parameter> getFreeParametersUsed()
    {
        return Collections.<Parameter> emptyList();
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
        output_info.setSingleton(true);
        this.setOutputInfo(output_info);
    }
    
    @Override
    public boolean isDataSourceCompliant(DataSourceMetaData metadata)
    {
        return true;
    }
    
    @Override
    public String toExplainString()
    {
        return super.toExplainString();
    }
    
    @Override
    public Operator accept(OperatorVisitor visitor)
    {
        return visitor.visitGround(this);
    }
    
    @Override
    public Operator copy()
    {
        Ground copy = new Ground();
        super.copy(copy);
        
        return copy;
    }
    
    /**
     * <b>Inherited JavaDoc:</b><br>
     * {@inheritDoc}
     * <br><br>
     * <b>See original method below.</b>
     * <br>
     * @see edu.ucsd.forward.query.logical.AbstractOperator#copyWithoutType()
     */
    @Override
    public Operator copyWithoutType()
    {
        return new Ground();
    }
    
}
