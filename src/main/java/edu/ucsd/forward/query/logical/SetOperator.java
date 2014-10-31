package edu.ucsd.forward.query.logical;

import edu.ucsd.forward.data.TypeUtil;
import edu.ucsd.forward.data.source.DataSourceMetaData;
import edu.ucsd.forward.exception.ExceptionMessages;
import edu.ucsd.forward.query.QueryCompilationException;
import edu.ucsd.forward.query.ast.SetOpExpression.SetOpType;
import edu.ucsd.forward.query.ast.SetQuantifier;
import edu.ucsd.forward.query.logical.term.Parameter;
import edu.ucsd.forward.query.logical.term.RelativeVariable;
import edu.ucsd.forward.query.logical.term.Term;
import edu.ucsd.forward.query.logical.term.Variable;
import edu.ucsd.forward.query.logical.visitors.OperatorVisitor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * Represents the union, intersect and except logical operators.
 * 
 * @author Michalis Petropoulos
 * @author Yupeng Fu
 */
@SuppressWarnings("serial")
public class SetOperator extends AbstractOperator
{
    private SetOpType      m_type;
    
    private SetQuantifier  m_set_quantifier;
    
    // Kevin: The match vars are used for SPEC_KEY_UNION operation.
    private List<Term>     m_match_terms;
    
    private List<Variable> m_used_variables;
    
    /**
     * Initializes an instance of the operator.
     * 
     * @param type
     *            the set operation type.
     * @param set_quantifier
     *            a set quantifier.
     */
    public SetOperator(SetOpType type, SetQuantifier set_quantifier)
    {
        super();
        
        assert (type != null);
        m_type = type;
        
        assert (set_quantifier != null);
        m_set_quantifier = set_quantifier;
        
        m_used_variables = new ArrayList<Variable>();
    }
    
    /**
     * Default constructor.
     */
    @SuppressWarnings("unused")
    private SetOperator()
    {
    }
    
    @Override
    public String getName()
    {
        return m_type.name();
    }
    
    /**
     * Returns the set operation type.
     * 
     * @return the set operation type.
     */
    public SetOpType getSetOpType()
    {
        return m_type;
    }
    
    /**
     * Sets the set operation type.
     * 
     * @param type
     *            the set operation type
     */
    public void setSetOpType(SetOpType type)
    {
        assert type != null;
        m_type = type;
    }
    
    /**
     * Gets the set quantifier.
     * 
     * @return the set quantifier.
     */
    public SetQuantifier getSetQuantifier()
    {
        return m_set_quantifier;
    }
    
    @Override
    public List<Variable> getVariablesUsed()
    {
        return m_used_variables;
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
        
        if (m_type == SetOpType.OUTER_UNION)
        {
            // Iterate over the input variables
            for (RelativeVariable var : this.getInputVariables())
            {
                // Add the variable to the output info only if it's not already there. If a variable appears in two child
                // operators, then the two types are identical, otherwise an exception would have been raised by now.
                if (!output_info.contains(var))
                {
                    // Add output info entry
                    output_info.add(var, var);
                    m_used_variables.add(var);
                }
            }
        }
        else
        {
            // Get the left and right info
            OutputInfo left_info = this.getChildren().get(0).getOutputInfo();
            OutputInfo right_info = this.getChildren().get(1).getOutputInfo();
            
            // Check that the schemas are isomorphic
            Iterator<RelativeVariable> left_iter = left_info.getVariables().iterator();
            Iterator<RelativeVariable> right_iter = right_info.getVariables().iterator();
            boolean isomorphic = true;
            while (left_iter.hasNext())
            {
                RelativeVariable left_var = left_iter.next();
                if (!right_iter.hasNext())
                {
                    isomorphic = false;
                    break;
                }
                RelativeVariable right_var = right_iter.next();
                if (!TypeUtil.deepEqualsByIsomorphism(left_var.getType(), right_var.getType()))
                {
                    isomorphic = false;
                    break;
                }
            }
            if (right_iter.hasNext())
            {
                isomorphic = false;
            }
            if (!isomorphic)
            {
                throw new QueryCompilationException(ExceptionMessages.QueryCompilation.DIFFERENT_UNION_TYPE,
                                                    left_info.getVariables().iterator().next().getLocation());
            }
            
            // Copy the input variables of the left child
            // FIXME Resolve NullTypes
            for (RelativeVariable var : left_info.getVariables())
            {
                output_info.add(var, var);
            }
            
            m_used_variables.clear();
            for (Variable var : left_info.getVariables())
            {
                m_used_variables.add(var);
            }
            
            if (m_match_terms != null)
            {
                for (Term term : m_match_terms)
                {
                    term.inferType(getChildren());
                    m_used_variables.addAll(term.getVariablesUsed());
                }
            }
        }
        
        // Set the actual output info
        this.setOutputInfo(output_info);
    }
    
    @Override
    public boolean isDataSourceCompliant(DataSourceMetaData metadata)
    {
        switch (this.getSetOpType())
        {
            case UNION:
            case INTERSECT:
            case EXCEPT:
                break;
            case OUTER_UNION:
                switch (metadata.getDataModel())
                {
                    case SQLPLUSPLUS:
                        return super.isDataSourceCompliant(metadata);
                    case RELATIONAL:
                        // Never compliant with relational data sources
                        return false;
                    default:
                        throw new AssertionError();
                }
            default:
                throw new AssertionError();
        }
        return super.isDataSourceCompliant(metadata);
    }
    
    @Override
    public String toExplainString()
    {
        String str = super.toExplainString() + " ";
        
        str += m_type.name() + " " + m_set_quantifier.name();
        
        return str;
    }
    
    @Override
    public Operator accept(OperatorVisitor visitor)
    {
        return visitor.visitSetOperator(this);
    }
    
    @Override
    public Operator copy()
    {
        SetOperator copy = new SetOperator(m_type, m_set_quantifier);
        super.copy(copy);
        
        return copy;
    }

    @Override
    public Operator copyWithoutType()
    {
        SetOperator copy = new SetOperator(m_type, m_set_quantifier);

        return copy;
    }

    /**
     * Replace the left child with the given operator. This method is useful in plan rewriting.
     * 
     * @param left
     *            the operator to replace.
     */
    public void replaceLeftChild(Operator left)
    {
        assert left != null;
        assert getChildren().size() == 2;
        Operator old_left = getChildren().get(0);
        Operator old_right = getChildren().get(1);
        removeChild(old_left);
        removeChild(old_right);
        addChild(left);
        addChild(old_right);
    }
    
    /**
     * Replace the right child with the given operator. This method is useful in plan rewriting.
     * 
     * @param right
     *            the operator to replace.
     */
    public void replaceRightChild(Operator right)
    {
        assert right != null;
        assert getChildren().size() == 2;
        Operator old_right = getChildren().get(1);
        removeChild(old_right);
        addChild(right);
    }
    
    /**
     * Returns the match terms.
     * 
     * @return the match terms.
     */
    public List<Term> getMatchTerms()
    {
        return m_match_terms;
    }
    
}
