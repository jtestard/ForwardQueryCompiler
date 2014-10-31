/**
 * 
 */
package edu.ucsd.forward.data.diff;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import edu.ucsd.app2you.util.identity.DeepEquality;
import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.ContextUtil;
import edu.ucsd.forward.data.DataPath;
import edu.ucsd.forward.data.TypeUtil;
import edu.ucsd.forward.data.ValueUtil;
import edu.ucsd.forward.data.type.ScalarType;
import edu.ucsd.forward.data.value.CollectionValue;
import edu.ucsd.forward.data.value.DataTree;
import edu.ucsd.forward.data.value.NullValue;
import edu.ucsd.forward.data.value.ScalarValue;
import edu.ucsd.forward.data.value.SwitchValue;
import edu.ucsd.forward.data.value.TupleValue;
import edu.ucsd.forward.data.value.Value;
import edu.ucsd.forward.data.value.TupleValue.AttributeValueEntry;

/**
 * A data comparer compares two data nodes recursively and produces a list of diffs.
 * 
 * FIXME: Review this class, and implement test cases.
 * 
 * @author Kian Win Ong
 * 
 */
public class DataComparer
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(DataComparer.class);
    
    private Value               m_from;
    
    private Value               m_to;
    
    /**
     * Constructs a data comparer. Both values must be part of immutable data trees. The two values may belong either to the same
     * data tree, or two separate data trees.
     * 
     * @param from
     *            the from value.
     * @param to
     *            the to value.
     */
    public DataComparer(Value from, Value to)
    {
        assert (from != null) : "From value must be non-null";
        assert (to != null) : "To value must be non-null";
        
        assert (from.getClass().equals(to.getClass()));
        assert (TypeUtil.deepEqualsByIsomorphism(from.getType(), to.getType()));
        // assert (from.getDataTree().isDeepImmutable()) : "Value must be part of immutable data tree";
        // assert (to.getDataTree().isDeepImmutable()) : "Value must be part of immutable data tree";
        
        m_from = from;
        m_to = to;
    }
    
    /**
     * Compares the two value and returns a list of diffs.
     * 
     * @return the diffs.
     */
    public List<Diff> compare()
    {
        return compareValues(m_from, m_to);
        // return Arrays.<Diff>asList(new ReplaceDiff(m_to));
    }
    
    /**
     * Compares two values.
     * 
     * @param from
     *            - the from value.
     * @param to
     *            - the to value.
     * @return the diffs.
     */
    protected static List<Diff> compareValues(Value from, Value to)
    {
        // Ugly proxying code
        
        if (from instanceof DataTree)
        {
            assert (to instanceof DataTree);
            return compareDataTrees((DataTree) from, (DataTree) to);
        }
        else if (from instanceof TupleValue)
        {
            assert (to instanceof TupleValue || to instanceof NullValue);
            if (to instanceof TupleValue)
            {
                return compareTuple((TupleValue) from, (TupleValue) to);
            }
            else
            {
                return Arrays.<Diff> asList(new ReplaceDiff((Value) to));
            }
        }
        else if (from instanceof CollectionValue)
        {
            assert (to instanceof CollectionValue || to instanceof NullValue);
            if (to instanceof CollectionValue)
            {
                return compareRelation((CollectionValue) from, (CollectionValue) to);
            }
            else
            {
                return Arrays.<Diff> asList(new ReplaceDiff((Value) to));
            }
        }
        else if (from instanceof SwitchValue)
        {
            assert (to instanceof SwitchValue || to instanceof NullValue);
            if (to instanceof SwitchValue)
            {
                return compareSwitch((SwitchValue) from, (SwitchValue) to);
            }
            else
            {
                return Arrays.<Diff> asList(new ReplaceDiff((Value) to));
            }
        }
        else if (from instanceof ScalarValue)
        {
            assert (to instanceof ScalarValue || to instanceof NullValue);
            
            if (ValueUtil.deepEquals(from, to))
            {
                return Collections.emptyList();
            }
            else
            {
                return Arrays.<Diff> asList(new ReplaceDiff((Value) to));
            }
        }
        else
        {
            assert (from instanceof NullValue);
            
            if (to instanceof NullValue)
            {
                return Collections.emptyList();
            }
            
            return Arrays.<Diff> asList(new ReplaceDiff((Value) to));
        }
    }
    
    /**
     * Compares two data trees.
     * 
     * @param from_data_tree
     *            - the from data tree.
     * @param to_data_tree
     *            - the to data tree.
     * @return the diffs.
     */
    protected static List<Diff> compareDataTrees(DataTree from_data_tree, DataTree to_data_tree)
    {
        return compareValues(from_data_tree.getRootValue(), to_data_tree.getRootValue());
    }
    
    /**
     * Compares two relations.
     * 
     * FIXME This version of compareRelation takes into consideration ordering, thus it will generate the necessary diffs to ensure
     * that the patched collection has the same order as the target collection. The original method compareRelationOriginal should
     * be used once we have a way to differentiate sets and lists.
     * 
     * @param from_relation
     *            from
     * @param to_relation
     *            target
     * @return diffs
     */
    protected static List<Diff> compareRelation(CollectionValue from_relation, CollectionValue to_relation)
    {
        // For every tuple in the target collection, keep advancing the pointer in the source collection
        // until a tuple with the same data path is found. Anything skipped over when advancing the pointer
        // will be deleted. This is so that non-consecutive tuples in the source collection that are in the correct order will be
        // preserved.
        
        int i = 0;
        int j = 0;
        List<Diff> replace_diffs = new ArrayList<Diff>();
        List<Diff> delete_diffs = new ArrayList<Diff>();
        for (j = 0; j < to_relation.getTuples().size(); j++, i++)
        {
            if (i >= from_relation.getTuples().size())
            {
                break;
            }
            TupleValue from_tuple = null;
            TupleValue to_tuple = to_relation.getTuples().get(j);
            DataPath to_data_path = new DataPath(to_tuple);
            do
            {
                from_tuple = from_relation.getTuples().get(i);
                DataPath from_data_path = new DataPath(from_tuple);
                if (to_data_path.equals(from_data_path))
                {
                    break;
                }
                delete_diffs.add(new DeleteDiff(from_data_path));
                i++;
            } while (i < from_relation.getTuples().size());
            
            if (i >= from_relation.getTuples().size())
            {
                break;
            }
            
            // The data paths match, so we can just keep track of
            // the necessary replace diffs.
            List<Diff> tuple_diffs = compareTuple(from_tuple, to_tuple);
            if (tuple_diffs.size() > 0)
            {
                replace_diffs.addAll(tuple_diffs);
            }
        }
        List<Diff> diffs = new ArrayList<Diff>();
        // Delete remaining tuples that are not skipped or matched
        for (; i < from_relation.getTuples().size(); i++)
        {
            TupleValue from_tuple = from_relation.getTuples().get(i);
            delete_diffs.add(new DeleteDiff(from_tuple));
        }
        // Insert new tuples that are not matched
        List<Diff> insert_diffs = new ArrayList<Diff>();
        for (; j < to_relation.getTuples().size(); j++)
        {
            TupleValue to_tuple = to_relation.getTuples().get(j);
            insert_diffs.add(new InsertDiff(to_tuple));
        }
        
        // TODO Add heuristic to check whether the size of the diffs required for replace, delete and insert
        // is larger than just replacing the entire collection.
        // The client side will also need to be able to handle collection replacements.
        
        // No diffs are needed for empty collections
        if (from_relation.getTuples().size() == 0 && to_relation.getTuples().size() == 0)
        {
            diffs.clear();
        }
        
        // If we are deleting everything in the source, then just issue a replace diff
        // on the whole relation.
        else if (delete_diffs.size() == from_relation.getTuples().size())
        {
            // FIXME : Not always the best solution because perhaps there is no
            // replace diff for the relation, in which case the simulator will have to
            // create a delete/insert on the collection, if that fails it might end up
            // having to destroy the unit and then re-add it.
            ReplaceDiff replace_diff = new ReplaceDiff(new DataPath(from_relation), to_relation);
            diffs.add(replace_diff);
        }
        else
        {
            // Use the replace, delete and insert diffs
            diffs.addAll(replace_diffs);
            diffs.addAll(delete_diffs);
            diffs.addAll(insert_diffs);
        }
        return diffs;
    }
    
    /**
     * Compares two relations.
     * 
     * @param from_relation
     *            - the from relation.
     * @param to_relation
     *            - the to relation.
     * @return the diffs.
     */
    protected static List<Diff> compareRelationOriginal(CollectionValue from_relation, CollectionValue to_relation)
    {
        List<Diff> diffs = new ArrayList<Diff>();
        
        List<TupleContext> from_context_list = getTupleContexts(from_relation);
        List<TupleContext> to_context_list = getTupleContexts(to_relation);
        
        Set<TupleContext> from_contexts_set = new LinkedHashSet<TupleContext>(from_context_list);
        Set<TupleContext> to_contexts_set = new LinkedHashSet<TupleContext>(to_context_list);
        
        // Find tuples that no longer exist in the to-relation, and produce delete diffs
        for (int i = 0; i < from_relation.getTuples().size(); i++)
        {
            TupleValue from_tuple = from_relation.getTuples().get(i);
            TupleContext from_context = from_context_list.get(i);
            
            // A delete diff for each missing tuple
            if (!to_contexts_set.contains(from_context))
            {
                diffs.add(new DeleteDiff(from_tuple));
            }
        }
        
        /*
         * FIXME: We assume here that the relative ordering of tuples has not changed. To capture the general case, we need a
         * MoveCommand for re-ordering sub-trees.
         */

        // Find tuples that are new in the to-relation, and produce insert diffs
        int n = to_relation.getTuples().size();
        for (int i = 0; i < n; i++)
        {
            // Skip existing tuples
            if (from_contexts_set.contains(to_context_list.get(i))) continue;
            
            // Found a new tuple
            
            // Find the range of new tuples
            int j = -1;
            for (j = i + 1; j < n; j++)
            {
                if (from_contexts_set.contains(to_context_list.get(j))) break;
            }
            
            // The range of new tuples is in [i, j)
            
            // The after context is the tuple immediately preceding i
            // ContextValue after_context = (i == 0) ? null : ContextUtil.getContextAsValue(to_relation.getTuples().get(i - 1));
            
            // The before context is the tuple j
            // ContextValue before_context = (j == n) ? null : ContextUtil.getContextAsValue(to_relation.getTuples().get(j));
            
            // The payload tuples are those in range [i, j)
            List<TupleValue> payloads = new ArrayList<TupleValue>();
            for (int k = i; k < j; k++)
            {
                payloads.add(to_relation.getTuples().get(k));
            }
            
            // Produce the insert diff
            for (TupleValue payload : payloads)
            {
                diffs.add(new InsertDiff(payload));
            }
            
            // Fast-forward i
            i = j;
        }
        
        // Find tuples that exist in both the from-relation and to-relation
        for (int i = 0; i < from_relation.getTuples().size(); i++)
        {
            TupleValue from_tuple = from_relation.getTuples().get(i);
            TupleContext from_context = from_context_list.get(i);
            
            for (int j = 0; j < to_relation.getTuples().size(); j++)
            {
                TupleValue to_tuple = to_relation.getTuples().get(j);
                TupleContext to_context = to_context_list.get(j);
                
                if (!from_context.equals(to_context)) continue;
                
                // Found tuples with identical context - recurse and compare the tuples
                diffs.addAll(compareTuple(from_tuple, to_tuple));
            }
            
        }
        return diffs;
    }
    
    /**
     * Compares two tuples.
     * 
     * @param from_tuple
     *            - the from tuple.
     * @param to_tuple
     *            - the to tuple.
     * @return the diffs.
     */
    protected static List<Diff> compareTuple(TupleValue from_tuple, TupleValue to_tuple)
    {
        HashMap<String, List<Diff>> diff_map = new LinkedHashMap<String, List<Diff>>();
        // Produce diffs for non-scalar attributes
        List<Diff> diffs = new ArrayList<Diff>();
        for (AttributeValueEntry entry : from_tuple)
        {
            Value from_value = entry.getValue();
            Value to_value = to_tuple.getAttribute(entry.getName());
            if (from_value.getType() instanceof ScalarType) continue;
            
            diff_map.put(entry.getName(), compareValues(from_value, to_value));
        }
        
        // Optimization: if each of the non-scalar attributes produces a replace diff, simply replace the entire tuple
        if (areAllReplaceDiffs(diffs, to_tuple))
        {
            // FIXME: should check for a flag before using this optimization
            diffs.clear();
            diffs.add(new ReplaceDiff(to_tuple));
        }
        
        // Otherwise compare the scalar attributes
        else
        {
            for (AttributeValueEntry entry : from_tuple)
            {
                Value from_value = entry.getValue();
                Value to_value = to_tuple.getAttribute(entry.getName());
                if (!(from_value.getType() instanceof ScalarType)) continue;
                
                diff_map.put(entry.getName(), compareValues(from_value, to_value));
            }
            
            // Preserve the order of the diffs according to the order of the attributes
            for (String attribute_name : from_tuple.getAttributeNames())
            {
                if (diff_map.containsKey(attribute_name))
                {
                    diffs.addAll(diff_map.get(attribute_name));
                }
            }
        }
        
        return diffs;
    }
    
    /**
     * Compares two switch values.
     * 
     * @param from_switch_value
     *            - the from switch value.
     * @param to_switch_value
     *            - the to switch value.
     * @return the diffs.
     */
    protected static List<Diff> compareSwitch(SwitchValue from_switch_value, SwitchValue to_switch_value)
    {
        if (from_switch_value.getCaseName().equals(to_switch_value.getCaseName()))
        {
            // FIXME do not handle comparing null tuple
            return compareTuple((TupleValue) from_switch_value.getCase(), (TupleValue) to_switch_value.getCase());
        }
        else
        {
            return Arrays.<Diff> asList(new ReplaceDiff(to_switch_value));
        }
    }
    
    /**
     * Determines whether the given diffs are replace diffs for each of the tuple's attributes.
     * 
     * @param diffs
     *            - the diffs.
     * @param tuple
     *            - the tuple.
     * @return <code>true</code> if the given diffs are replace diffs for each of the tuple's attributes; <code>false</code>
     *         otherwise.
     */
    protected static boolean areAllReplaceDiffs(List<Diff> diffs, TupleValue tuple)
    {
        // FIXME
        return false;
    }
    
    /**
     * Returns the tuple contexts for a relation.
     * 
     * @param relation
     *            - the relation.
     * @return the tuple contexts.
     */
    protected static List<TupleContext> getTupleContexts(CollectionValue relation)
    {
        List<TupleContext> list = new ArrayList<TupleContext>();
        for (TupleValue tuple : relation.getTuples())
        {
            list.add(new TupleContext(tuple));
        }
        return list;
    }
    
    /**
     * Convenience wrapper around <code>ContextValue</code> that uses equality of sets of values.
     * 
     * @author Kian Win
     * 
     */
    private static class TupleContext implements DeepEquality
    {
        private List<ScalarValue> m_scalar_values;
        
        /**
         * Constructs a tuple context.
         * 
         * @param tuple
         *            - the tuple.
         */
        public TupleContext(TupleValue tuple)
        {
            assert (tuple != null);
            assert tuple.getParent() instanceof CollectionValue;
            m_scalar_values = ContextUtil.getContextAsValue(tuple).getScalarValues();
        }
        
        public List<ScalarValue> getScalarValues()
        {
            return m_scalar_values;
        }
        
        /**
         * <b>Inherited JavaDoc:</b><br> {@inheritDoc} <br>
         * <br>
         * <b>See original method below.</b> <br>
         * 
         * @see edu.ucsd.app2you.util.identity.DeepEquality#getDeepEqualityObjects()
         */
        @Override
        public Object[] getDeepEqualityObjects()
        {
            return new Object[] { m_scalar_values };
        }
        
        /**
         * <b>Inherited JavaDoc:</b><br> {@inheritDoc} <br>
         * <br>
         * <b>See original method below.</b> <br>
         * 
         * @see java.lang.Object#equals(java.lang.Object)
         */
        @Override
        public boolean equals(Object o)
        {
            if (!(o instanceof TupleContext)) return false;
            if (o == null) return false;
            if (this == o) return true;
            
            return ValueUtil.deepEquals(new HashSet<ScalarValue>(m_scalar_values),
                                        new HashSet<ScalarValue>(((TupleContext) o).getScalarValues()));
        }
        
        /**
         * <b>Inherited JavaDoc:</b><br> {@inheritDoc} <br>
         * <br>
         * <b>See original method below.</b> <br>
         * 
         * @see java.lang.Object#hashCode()
         */
        @Override
        public int hashCode()
        {
            int code = 0;
            for (ScalarValue value : m_scalar_values)
            {
                code += value.toString().hashCode();
            }
            return code;
        }
    }
}
