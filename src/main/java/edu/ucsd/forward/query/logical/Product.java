package edu.ucsd.forward.query.logical;

import java.util.Collections;
import java.util.List;

import edu.ucsd.forward.query.QueryCompilationException;
import edu.ucsd.forward.query.logical.term.Parameter;
import edu.ucsd.forward.query.logical.term.RelativeVariable;
import edu.ucsd.forward.query.logical.term.Variable;
import edu.ucsd.forward.query.logical.visitors.OperatorVisitor;

/**
 * Represents the Cartesian product logical operator.
 * 
 * @author Michalis Petropoulos
 * 
 */
@SuppressWarnings("serial")
public class Product extends AbstractOperator
{
    /**
     * Initializes an instance of the operator.
     */
    public Product()
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
        
        // Copy the input variables
        for (RelativeVariable var : this.getInputVariables())
        {
            output_info.add(var, var);
        }
        
        // Set the actual output info
        this.setOutputInfo(output_info);
    }
    
    @Override
    public Operator accept(OperatorVisitor visitor)
    {
        return visitor.visitProduct(this);
    }
    
    @Override
    public Operator copy()
    {
        Product copy = new Product();
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
        return new Product();
    }
}
