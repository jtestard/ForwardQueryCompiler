/**
 * 
 */
package edu.ucsd.forward.query.logical;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.type.CollectionType;
import edu.ucsd.forward.data.type.TupleType;
import edu.ucsd.forward.data.type.Type;
import edu.ucsd.forward.exception.ExceptionMessages.QueryCompilation;
import edu.ucsd.forward.query.QueryCompilationException;
import edu.ucsd.forward.query.logical.term.Parameter;
import edu.ucsd.forward.query.logical.term.QueryPath;
import edu.ucsd.forward.query.logical.term.RelativeVariable;
import edu.ucsd.forward.query.logical.term.Variable;
import edu.ucsd.forward.query.logical.visitors.OperatorVisitor;

/**
 * Represents the logical operator of duplicate elimination.
 * 
 * @author Yupeng
 * 
 */
@SuppressWarnings("serial")
public class EliminateDuplicates extends AbstractUnaryOperator
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(EliminateDuplicates.class);
    
    private List<QueryPath>     m_conditions;
    
    /**
     * Initializes an instance of the operator.
     */
    public EliminateDuplicates()
    {
        super();
        
        m_conditions = new ArrayList<QueryPath>();
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
        
        // Normal form assumption: this operator is above a projection, and therefore input a tuple with a Binding with a single
        // attribute.
        assert (this.getInputVariables().size() == 1);
        RelativeVariable var = this.getInputVariables().get(0);
        output_info.add(var, var);
        output_info.setKeyTerms(this.getChild().getOutputInfo().getKeyTerms());
        
        m_conditions.clear();
        Type var_type = var.getType();
        if (var_type instanceof TupleType)
        {
            // Add each attribute as a condition
            for (String attr_name : ((TupleType) var_type).getAttributeNames())
            {
                Type attr_type = ((TupleType) var_type).getAttribute(attr_name);
                if (attr_type instanceof CollectionType || attr_type instanceof TupleType)
                {
                    // the attribute has a complex type
                    throw new QueryCompilationException(QueryCompilation.NON_SCALAR_DISTINCT_TYPE, var.getLocation());
                }
                QueryPath qp = new QueryPath(var, Arrays.asList(attr_name));
                m_conditions.add(qp);
            }
        }
        // the variable has a complex type
        else if (var_type instanceof CollectionType)
        {
            throw new QueryCompilationException(QueryCompilation.NON_SCALAR_DISTINCT_TYPE, var.getLocation());
        }
        else
        {
            m_conditions.add(new QueryPath(var, Collections.<String> emptyList()));
        }
        
        // Set the m_output_ordered flag
        output_info.setOutputOrdered(this.getChild().getOutputInfo().isOutputOrdered());
        
        // Set the actual output info
        this.setOutputInfo(output_info);
    }
    
    /**
     * Returns the list of query paths that should be used to compare bindings and remove duplicates.
     * 
     * @return the list of query paths
     */
    public List<QueryPath> getConditions()
    {
        return m_conditions;
    }
    
    @Override
    public List<Variable> getVariablesUsed()
    {
        return Arrays.asList((Variable) this.getInputVariables().get(0));
    }
    
    @Override
    public Operator accept(OperatorVisitor visitor)
    {
        return visitor.visitEliminateDuplicates(this);
    }
    
    @Override
    public Operator copy()
    {
        EliminateDuplicates copy = new EliminateDuplicates();
        super.copy(copy);
        
        return copy;
    }
    
}
