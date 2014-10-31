package edu.ucsd.forward.query.physical;

import java.util.ArrayList;
import java.util.List;

import edu.ucsd.forward.data.value.NullValue;
import edu.ucsd.forward.query.QueryExecutionException;
import edu.ucsd.forward.query.ast.SetQuantifier;
import edu.ucsd.forward.query.logical.OutputInfo;
import edu.ucsd.forward.query.logical.SetOperator;
import edu.ucsd.forward.query.logical.term.Term;
import edu.ucsd.forward.query.logical.term.Variable;
import edu.ucsd.forward.query.physical.visitor.OperatorImplVisitor;
import edu.ucsd.forward.query.suspension.SuspensionException;

/**
 * Implementation of the outer union logical operator.
 * 
 * @author Michalis Petropoulos
 * @author Yupeng
 */
public class OuterUnionImpl extends AbstractBinaryOperatorImpl<SetOperator>
{
    /**
     * The buffer for the child operator implementations distinct bindings.
     */
    private BindingBuffer m_index;
    
    /**
     * Constructs an instance of the operator implementation.
     * 
     * @param logical
     *            a logical join operator.
     * @param left_child
     *            the left child operator implementation.
     * @param right_child
     *            the right child operator implementation.
     */
    public OuterUnionImpl(SetOperator logical, OperatorImpl left_child, OperatorImpl right_child)
    {
        super(logical, left_child, right_child);
    }
    
    @Override
    public void open() throws QueryExecutionException
    {
        if (getOperator().getSetQuantifier() == SetQuantifier.DISTINCT)
        {
            OutputInfo in_info = getOperator().getOutputInfo();
            
            // Form conditions to be used by the binding buffer
            List<Term> conditions = new ArrayList<Term>();
            for (Variable var : in_info.getVariables())
            {
                conditions.add(var);
            }
            
            m_index = new BindingBufferHash(conditions, in_info, false);
        }
        super.open();
    }
    
    @Override
    public Binding next() throws QueryExecutionException, SuspensionException
    {
        OutputInfo out_info = getOperator().getOutputInfo();
        OutputInfo left_info = getLeftChild().getOperator().getOutputInfo();
        OutputInfo right_info = getRightChild().getOperator().getOutputInfo();
        
        Binding out_binding = null;
        // Try getting binding from left child
        out_binding = expandBinding(out_info, left_info, getLeftChild().next());
        if (getOperator().getSetQuantifier() == SetQuantifier.DISTINCT)
        {
            // Get the next distinct one
            while (out_binding != null)
            {
                // If there is no matching binding, then output the current one
                if (!m_index.get(out_binding, false).hasNext())
                {
                    m_index.add(out_binding);
                    break;
                }
                
                // Else fetch the next binding
                out_binding = expandBinding(out_info, left_info, getLeftChild().next());
            }
        }
        if (out_binding != null) return out_binding;
        
        // Left child exhausted, try right child
        out_binding = expandBinding(out_info, right_info, getRightChild().next());
        if (getOperator().getSetQuantifier() == SetQuantifier.DISTINCT)
        {
            // Get the next distinct one
            while (out_binding != null)
            {
                // If there is no matching binding, then output the current one
                if (!m_index.get(out_binding, false).hasNext())
                {
                    m_index.add(out_binding);
                    break;
                }
                
                // Else fetch the next binding
                out_binding = expandBinding(out_info, right_info, getRightChild().next());
            }
        }
        
        return out_binding;
    }
    
    /**
     * Expands the input binding according to the expected output info, if the attribute in the output does not exist in the input,
     * pad the output binding with null value.
     * 
     * @param out_info
     *            the output info
     * @param in_info
     *            the input info
     * @param in_binding
     *            the input binding
     * @return the expanded output binding
     */
    private Binding expandBinding(OutputInfo out_info, OutputInfo in_info, Binding in_binding)
    {
        if (in_binding == null)
        {
            return null;
        }
        
        Binding out_binding = new Binding();
        // Expand the binding
        for (int i = 0; i < out_info.size(); i++)
        {
            Variable out_var = out_info.getVariable(i);
            
            // Check if the attribute exists in the input
            if (in_info.hasVariable(out_var.getName()))
            {
                // Set the binding value from the input
                out_binding.addValue(in_binding.getValue(in_info.getVariableIndex(out_var)));
            }
            else
            {
                // Pad with null value
                out_binding.addValue(new BindingValue(new NullValue(), false));
            }
        }
        return out_binding;
    }
    
    @Override
    public void close() throws QueryExecutionException
    {
        if (this.getState() == State.OPEN)
        {
            if (getOperator().getSetQuantifier() == SetQuantifier.DISTINCT)
            {
                m_index.clear();
            }
        }
        
        super.close();
    }
    
    @Override
    public OperatorImpl copy(CopyContext context)
    {
        OuterUnionImpl copy = new OuterUnionImpl(this.getOperator(), this.getLeftChild().copy(context),
                                                 this.getRightChild().copy(context));
        
        return copy;
    }
    
    @Override
    public void accept(OperatorImplVisitor visitor)
    {
        visitor.visitOuterUnionImpl(this);
    }
    
}
