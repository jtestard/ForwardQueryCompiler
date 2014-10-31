/**
 * 
 */
package edu.ucsd.forward.data.diff;

import java.util.Arrays;
import java.util.List;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.DataPath;
import edu.ucsd.forward.data.ValueTypeMapUtil;
import edu.ucsd.forward.data.ValueUtil;
import edu.ucsd.forward.data.value.DataTree;
import edu.ucsd.forward.data.value.Value;

/**
 * A data tree patcher applies data diffs to a data tree.
 * 
 * @author Kian Win Ong
 * 
 */
public class DataPatcher
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(DataPatcher.class);
    
    /**
     * Patches a data tree. The data tree will be mutated, whereas the data diffs will remain the same.
     * 
     * @param data_tree
     *            the data tree.
     * @param data_diffs
     *            the data diffs.
     */
    public void patch(DataTree data_tree, Diff... data_diffs)
    {
        patch(data_tree, Arrays.asList(data_diffs));
    }
    
    /**
     * Patches a data tree. The data tree will be mutated, whereas the data diffs will remain the same.
     * 
     * @param data_tree
     *            the data tree.
     * @param data_diffs
     *            the data diffs.
     */
    public void patch(DataTree data_tree, List<Diff> data_diffs)
    {
        assert (data_tree != null);
        assert (!data_tree.isDeepImmutable());
        assert (data_diffs != null);
        
        // Nothing to do
        if (data_diffs.isEmpty()) return;
        
        // Apply each data diff in turn
        for (Diff data_diff : data_diffs)
        {
            applyDiff(data_tree, data_diff);
            
            /*
             * The current implementation of DataPath.find() requires the data tree to be type consistent. Therefore, after each
             * diff has been applied, the data tree needs to be re-checked for type consistency.
             * 
             * TODO: For performance, ideally type consistency is only checked once after all diffs have been applied.
             */

            ValueTypeMapUtil.map(data_tree, data_tree.getSchemaTree());
            
            // Too expensive to use during production
            // data_tree.checkConsistent();
            // assert (data_tree.isConsistent());
        }
    }
    
    /**
     * Applies a data diff to a data tree.
     * 
     * @param data_tree
     *            the data tree.
     * @param data_diff
     *            the data diff.
     */
    private void applyDiff(DataTree data_tree, Diff data_diff)
    {
        DataPath context = data_diff.getContext();
        
        if (data_diff instanceof InsertDiff)
        {
            // Clone the payload to avoid mutating the data diff
            Value payload = ValueUtil.cloneNoParentNoType(data_diff.getPayload());
            
            // Attach the payload to the data tree
            DataPath target_parent_context = context.up(1);
            Value target_parent_value = (Value) target_parent_context.find(data_tree);
            ValueUtil.attach(target_parent_value, context.getLastPathStep().getName(), payload);
        }
        else if (data_diff instanceof ReplaceDiff)
        {
            // Clone the payload to avoid mutating the data diff
            Value payload = ValueUtil.cloneNoParentNoType(data_diff.getPayload());
            
            // Attach the payload to the data tree, but only after detaching what it replaces
            Value target_value = context.find(data_tree);
            Value target_parent_value = (Value) target_value.getParent();
            ValueUtil.detach(target_value);
            ValueUtil.attach(target_parent_value, context.getLastPathStep().getName(), payload);
        }
        else if (data_diff instanceof DeleteDiff)
        {
            // Detach the old value
            Value target_value = context.find(data_tree);
            ValueUtil.detach(target_value);
        }
    }
    
}
