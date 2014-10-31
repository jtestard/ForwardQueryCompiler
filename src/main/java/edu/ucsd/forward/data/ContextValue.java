/**
 * 
 */
package edu.ucsd.forward.data;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import edu.ucsd.app2you.util.identity.DeepEquality;
import edu.ucsd.app2you.util.identity.EqualUtil;
import edu.ucsd.app2you.util.identity.HashCodeUtil;
import edu.ucsd.app2you.util.identity.IdentityHashSet;
import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.value.DataTree;
import edu.ucsd.forward.data.value.ScalarValue;

/**
 * A context value is the list of scalar values that comprises the context of a dataset. The list of scalar values matches the
 * global primary key attributes by ordinal position. The equality of a context value depends on reference-equality (instead of
 * object equality) of the scalar values, i.e. two context values are equal if and only if their respective scalar values are
 * identical nodes that come from the same dataset.
 * 
 * @author Kian Win
 * 
 */
public class ContextValue implements Iterable<ScalarValue>, DeepEquality
{
    @SuppressWarnings("unused")
    private static final Logger          log = Logger.getLogger(ContextValue.class);
    
    private DataTree                     m_dataset;
    
    private List<ScalarValue>            m_scalar_values;
    
    private IdentityHashSet<ScalarValue> m_equality_set;
    
    /**
     * Constructs a context value.
     * 
     * @param dataset
     *            - the dataset for the context.
     * @param scalar_values
     *            - the scalar values comprising the context.
     */
    public ContextValue(DataTree dataset, List<ScalarValue> scalar_values)
    {
        assert dataset != null;
        assert scalar_values != null;
        
        for (ScalarValue scalar_value : scalar_values)
        {
            // Each scalar value should be mapped to a type
            // FIXME: Should we simply require that the dataset is type consistent?
            assert scalar_value.getType() != null;
            
            // All scalar values must come from the same dataset
            assert scalar_value.getDataTree() == dataset;
        }
        
        m_dataset = dataset;
        m_scalar_values = scalar_values;
        
        // Add the scalar values to an identity hash set to help determine equality
        m_equality_set = new IdentityHashSet<ScalarValue>(m_scalar_values);
    }
    
    /**
     * <b>Inherited JavaDoc:</b><br> {@inheritDoc} <br>
     * <br>
     * <b>See original method below.</b> <br>
     * 
     * @see java.lang.Iterable#iterator()
     */
    @Override
    public Iterator<ScalarValue> iterator()
    {
        return m_scalar_values.iterator();
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
        // Use reference-equality of scalar values (instead of object equality)
        return new Object[] { m_equality_set };
    }
    
    /**
     * Returns the dataset.
     * 
     * @return the dataset.
     */
    public DataTree getDataset()
    {
        return m_dataset;
    }
    
    /**
     * Returns the list of scalar values comprising the context.
     * 
     * @return the list of scalar values comprising the context.
     */
    public List<ScalarValue> getScalarValues()
    {
        return Collections.unmodifiableList(m_scalar_values);
    }
    
    /**
     * Returns whether the scalar value is part of the context. Reference-equality is used for comparison.
     * 
     * @param scalar_value
     *            - the scalar value.
     * @return <code>true</code> if the identical scalar value object is part of the context; <code>false</code> otherwise.
     */
    public boolean contains(ScalarValue scalar_value)
    {
        return m_equality_set.contains(scalar_value);
    }
    
    /**
     * 
     * <b>Inherited JavaDoc:</b><br> {@inheritDoc} <br>
     * <br>
     * <b>See original method below.</b> <br>
     * 
     * @see java.lang.Object#equals(java.lang.Object)
     */
    @Override
    public boolean equals(Object o)
    {
        return EqualUtil.equalsByDeepEquality(this, o);
    }
    
    /**
     * 
     * <b>Inherited JavaDoc:</b><br> {@inheritDoc} <br>
     * <br>
     * <b>See original method below.</b> <br>
     * 
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode()
    {
        return HashCodeUtil.hashCodeByDeepEquality(this);
    }
    
    /**
     * 
     * <b>Inherited JavaDoc:</b><br> {@inheritDoc} <br>
     * <br>
     * <b>See original method below.</b> <br>
     * 
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString()
    {
        return m_scalar_values.toString();
    }
    
}
