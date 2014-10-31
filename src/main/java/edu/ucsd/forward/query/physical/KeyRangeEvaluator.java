/**
 * 
 */
package edu.ucsd.forward.query.physical;

import java.util.ArrayList;
import java.util.List;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.index.KeyRange;
import edu.ucsd.forward.data.value.ScalarValue;
import edu.ucsd.forward.query.QueryExecutionException;
import edu.ucsd.forward.query.logical.IndexScan.KeyRangeSpec;

/**
 * A key range evaluator that evaluates a list of key range specifications.
 * 
 * @author Yupeng
 * 
 */
public final class KeyRangeEvaluator
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(KeyRangeEvaluator.class);
    
    /**
     * Hidden constructor.
     */
    private KeyRangeEvaluator()
    {
        
    }
    
    /**
     * Evaluates the key ranges according to the input the binding.
     * 
     * @param binding
     *            the input binding.
     * @param specs
     *            the key range specifications.
     * @return the evaluated key ranges.
     * @throws QueryExecutionException
     *             if an exception is raised during term evaluation.
     */
    public static List<KeyRange> evaluate(Binding binding, List<KeyRangeSpec> specs) throws QueryExecutionException
    {
        List<KeyRange> ranges = new ArrayList<KeyRange>();
        // FIXME: currently only supports single key range
        assert specs.size() == 1;
        for (KeyRangeSpec spec : specs)
        {
            ScalarValue lower = null;
            if (spec.getLowerTerm() != null)
            {
                lower = (ScalarValue) TermEvaluator.evaluate(spec.getLowerTerm(), binding).getValue();
            }
            ScalarValue upper = null;
            if (spec.getUpperTerm() != null)
            {
                upper = (ScalarValue) TermEvaluator.evaluate(spec.getUpperTerm(), binding).getValue();
            }
            KeyRange range = KeyRange.bound(lower, upper, spec.isLowerOpen(), spec.isUpperOpen());
            ranges.add(range);
        }
        return ranges;
    }
}
