/**
 * 
 */
package edu.ucsd.forward.query.physical;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.query.function.Function;
import edu.ucsd.forward.query.function.FunctionRegistryException;
import edu.ucsd.forward.query.function.comparison.EqualFunction;
import edu.ucsd.forward.query.function.comparison.GreaterEqualFunction;
import edu.ucsd.forward.query.function.comparison.GreaterThanFunction;
import edu.ucsd.forward.query.function.comparison.LessEqualFunction;
import edu.ucsd.forward.query.function.comparison.LessThanFunction;
import edu.ucsd.forward.query.function.general.GeneralFunctionCall;
import edu.ucsd.forward.query.logical.OutputInfo;
import edu.ucsd.forward.query.logical.term.RelativeVariable;
import edu.ucsd.forward.query.logical.term.Term;
import edu.ucsd.forward.query.logical.term.Variable;

/**
 * Builds a binding buffer implementation.
 * 
 * @author Michalis Petropoulos
 * 
 */
public final class BindingBufferFactory
{
    @SuppressWarnings("unused")
    private static final Logger               log      = Logger.getLogger(BindingBufferFactory.class);
    
    private static final BindingBufferFactory INSTANCE = new BindingBufferFactory();
    
    /**
     * Constructor.
     */
    private BindingBufferFactory()
    {
    }
    
    /**
     * Gets the static instance.
     * 
     * @return the static instance.
     */
    public static BindingBufferFactory getInstance()
    {
        return INSTANCE;
    }
    
    /**
     * Builds a binding buffer implementation.
     * 
     * @param conditions
     *            a set of conditions to search for an indexable one.
     * @param left_info
     *            the output info of the bindings being indexed.
     * @param right_info
     *            the output info of the bindings probing the index.
     * @param track_matched_bindings
     *            determines whether to keep track of matched bindings.
     * @return a binding buffer implementation.
     */
    public BindingBuffer buildBindingBuffer(Collection<Term> conditions, OutputInfo left_info, OutputInfo right_info,
            boolean track_matched_bindings)
    {
        // The variables used by indexing and probing conditions
        Set<RelativeVariable> left_child = left_info.getVariables();
        Set<RelativeVariable> right_child = right_info.getVariables();
        
        // Loop over the conditions and look for equality conditions
        List<Term> left_terms = new ArrayList<Term>();
        List<Term> right_terms = new ArrayList<Term>();
        List<Term> used_conditions = new ArrayList<Term>();
        for (Term term : conditions)
        {
            if (!(term instanceof GeneralFunctionCall)) continue;
            
            if (!(((GeneralFunctionCall) term).getFunction() instanceof EqualFunction)) continue;
            
            GeneralFunctionCall f_call = (GeneralFunctionCall) term;
            Term left_term = f_call.getArguments().get(0);
            Term right_term = f_call.getArguments().get(1);
            
            List<Variable> left_vars = left_term.getVariablesUsed();
            List<Variable> right_vars = right_term.getVariablesUsed();
            
            if (left_vars.isEmpty() || right_vars.isEmpty()) continue;
            
            if (left_child.containsAll(left_vars) && right_child.containsAll(right_vars))
            {
                left_terms.add(left_term);
                right_terms.add(right_term);
                used_conditions.add(term);
            }
            else if (left_child.containsAll(right_vars) && right_child.containsAll(left_vars))
            {
                left_terms.add(right_term);
                right_terms.add(left_term);
                used_conditions.add(term);
            }
        }
        
        // Found equality conditions
        if (!used_conditions.isEmpty())
        {
            List<Term> remaining_conditions = new ArrayList<Term>(conditions);
            remaining_conditions.removeAll(used_conditions);
            
            // Return hash buffer
            return new BindingBufferHash(left_terms, right_terms, remaining_conditions, left_info, right_info,
                                         track_matched_bindings);
        }
        
        // Loop over the conditions and look for an arithmetic predicate (<, <=, >, >=)
        GeneralFunctionCall predicate = null;
        for (Term term : conditions)
        {
            if (!isAcceptableFunctionCall(term)) continue;
            
            GeneralFunctionCall f_call = (GeneralFunctionCall) term;
            Term left_term = f_call.getArguments().get(0);
            Term right_term = f_call.getArguments().get(1);
            
            List<Variable> left_paths = left_term.getVariablesUsed();
            List<Variable> right_paths = right_term.getVariablesUsed();
            
            if (left_child.containsAll(left_paths) && right_child.containsAll(right_paths))
            {
                predicate = f_call;
                break;
            }
            else if (left_child.containsAll(right_paths) && right_child.containsAll(left_paths))
            {
                predicate = invertPredicate(f_call);
                break;
            }
            
            predicate = null;
        }
        
        // Found an indexable predicate
        if (predicate != null)
        {
            // Return red black buffer
            return new BindingBufferRedBlack(predicate, conditions, left_info, right_info, track_matched_bindings);
        }
        
        // No indexable predicate found, so return the list
        return new BindingBufferList(conditions, left_info, right_info, track_matched_bindings);
    }
    
    /**
     * Determines if the input conjunct in the join condition is a function call that can be used to build an index.
     * 
     * @param term
     *            a conjunct in the join condition
     * @return true, if the conjunct can be used to build an index; false, otherwise;
     */
    private static boolean isAcceptableFunctionCall(Term term)
    {
        if (!(term instanceof GeneralFunctionCall)) return false;
        
        GeneralFunctionCall f_call = (GeneralFunctionCall) term;
        Function function = f_call.getFunction();
        
        if (function instanceof EqualFunction) return true;
        
        if (function instanceof GreaterEqualFunction) return true;
        
        if (function instanceof GreaterThanFunction) return true;
        
        if (function instanceof LessEqualFunction) return true;
        
        if (function instanceof LessThanFunction) return true;
        
        return false;
    }
    
    /**
     * Inverts the arguments of a function call and changes the function accordingly.
     * 
     * @param f_call
     *            the function call to invert.
     * @return the inverted function call.
     */
    private GeneralFunctionCall invertPredicate(GeneralFunctionCall f_call)
    {
        Term left_term = f_call.getArguments().get(0);
        Term right_term = f_call.getArguments().get(1);
        
        Function function = f_call.getFunction();
        
        try
        {
            if (function instanceof EqualFunction)
            {
                return new GeneralFunctionCall(EqualFunction.NAME, right_term, left_term);
            }
            
            if (function instanceof GreaterEqualFunction)
            {
                return new GeneralFunctionCall(LessEqualFunction.NAME, right_term, left_term);
            }
            
            if (function instanceof GreaterThanFunction)
            {
                return new GeneralFunctionCall(LessThanFunction.NAME, right_term, left_term);
            }
            
            if (function instanceof LessEqualFunction)
            {
                return new GeneralFunctionCall(GreaterEqualFunction.NAME, right_term, left_term);
            }
            
            if (function instanceof LessThanFunction)
            {
                return new GeneralFunctionCall(GreaterThanFunction.NAME, right_term, left_term);
            }
        }
        catch (FunctionRegistryException e)
        {
            // This should never happen
            throw new AssertionError();
        }
        
        throw new AssertionError();
    }
    
}
