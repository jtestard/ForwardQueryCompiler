/**
 * 
 */
package edu.ucsd.forward.query.logical.term;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.query.function.AbstractFunctionCall;

/**
 * Utility methods to manipulate terms.
 * 
 * @author Romain
 */
public final class TermUtil
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(TermUtil.class);
    
    /**
     * Private constructor.
     * 
     */
    private TermUtil()
    {
        
    }
    
    /**
     * Applies a map of substitutions to a given term and returns the rewritten term.
     * 
     * @param term
     *            the term on which to apply the substitutions
     * @param substitutions
     *            the map of substitutions
     * @return a new term with the substituted variables, or the original term if no changes were applied
     */
    public static Term substitute(Term term, Map<Term, Term> substitutions)
    {
        // First look for a direct match in the map of substitutions
        for (Term key : substitutions.keySet())
        {
            if (key.getClass() == term.getClass() && key.toExplainString().equals(term.toExplainString()))
            {
                Term result = substitutions.get(key);
                result = result.copy();
                result.setLocation(term.getLocation());
                return result;
            }
        }
        
        // Otherwise, handle special cases (nested terms)
        if (term instanceof QueryPath)
        {
            // Recursively substitute inside the term of the query path
            Term rewritten_term = substitute(((QueryPath) term).getTerm(), substitutions);
            if (rewritten_term == ((QueryPath) term).getTerm())
            {
                return term;
            }
            else
            {
                QueryPath result = new QueryPath(rewritten_term, ((QueryPath) term).getPathSteps());
                result.setType(term.getType());
                result.setLocation(term.getLocation());
                return result;
            }
        }
        else if (term instanceof Parameter)
        {           
            // Recursively substitute inside the term of the parameter
            Term rewritten_term = substitute(((Parameter) term).getTerm(), substitutions);
            if (rewritten_term == ((Parameter) term).getTerm())
            {
                return term;
            }
            else
            {
                Parameter result = new Parameter(rewritten_term);
                result.setType(term.getType());
                result.setLocation(term.getLocation());
                return result;
            }
        }
        else if (term instanceof AbstractFunctionCall<?>)
        {            
            // Recursively substitute inside the arguments of the function
            boolean changed = false;
            AbstractFunctionCall<?> func_call = (AbstractFunctionCall<?>) term.copy();
            List<Term> arguments = new ArrayList<Term>(func_call.getArguments());
            for(Term arg: arguments)
            {
                func_call.removeArgument(arg);
            }
            for (int i = 0; i < arguments.size(); i++)
            {
                Term arg = arguments.get(i);
                Term new_arg = substitute(arg, substitutions);
                new_arg.setLocation(arg.getLocation());
                if (new_arg != arg)
                {
                    changed = true;
                }
                func_call.addArgument(new_arg);
            }
            func_call.setLocation(term.getLocation());
            
            if (changed)
            {
                return func_call;
            }
            else
            {
                return term;
            }
        }

        // No substitution to apply, return the original term
        return term;
    }
}
