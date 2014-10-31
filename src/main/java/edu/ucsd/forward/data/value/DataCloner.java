/**
 * 
 */
package edu.ucsd.forward.data.value;

import java.sql.Date;
import java.sql.Timestamp;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.TypeUtil;
import edu.ucsd.forward.data.ValueTypeMapUtil;
import edu.ucsd.forward.data.type.Type;
import edu.ucsd.forward.data.value.TupleValue.AttributeValueEntry;

/**
 * A cloner for data trees.
 * 
 * @author Kian Win
 * @author Michalis Petropoulos
 * 
 */
public class DataCloner
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(DataCloner.class);
    
    private boolean             m_type_cloning;
    
    /**
     * Constructs a data cloner.
     */
    public DataCloner()
    {
        setTypeCloning(false);
    }
    
    /**
     * Returns whether the associated type tree and constraints are cloned.
     * 
     * @return <code>true</code> if the associated type tree and constraints are cloned; <code>false</code> otherwise.
     */
    public boolean hasTypeCloning()
    {
        return m_type_cloning;
    }
    
    /**
     * Sets whether the associated type tree and constraints are cloned.
     * 
     * @param type_cloning
     *            whether the associated type tree and constraints are cloned.
     */
    public void setTypeCloning(boolean type_cloning)
    {
        m_type_cloning = type_cloning;
    }
    
    /**
     * Clones the data sub-tree. Only the data node and its descendants will be deep-copied; its ancestors in the data tree will
     * not.
     * 
     * @param <T>
     *            the data class.
     * @param value
     *            the data node
     * @return the cloned data sub-tree.
     */
    public <T extends Value> T clone(T value)
    {
        T value_copy = cloneManual(value);
        if (hasTypeCloning() && value.getType() != null)
        {
            Type type_copy = TypeUtil.clone(value.getType());
            ValueTypeMapUtil.map(value_copy, type_copy);
        }
        return value_copy;
    }
    
    /**
     * Clones the data sub-tree by manually traversing the data tree and calling respective constructors. Only the value and its
     * descendants will be deep-copied; its ancestors in the data tree will not.
     * 
     * @param <T>
     *            the value class.
     * @param value
     *            the value.
     * @return the cloned data sub-tree.
     */
    @SuppressWarnings("unchecked")
    protected <T extends Value> T cloneManual(T value)
    {
        assert value != null;
        
        if (value instanceof DataTree)
        {
            DataTree old_dataset = (DataTree) value;
            DataTree new_dataset = new DataTree(cloneManual(old_dataset.getRootValue()));
            
            new_dataset.setTypeConsistent(old_dataset.isTypeConsistent());
            new_dataset.setConstraintConsistent(old_dataset.isConstraintConsistent());
            
            return (T) new_dataset;
        }
        else if (value instanceof TupleValue)
        {
            TupleValue old_tuple = (TupleValue) value;
            TupleValue new_tuple = new TupleValue();
            for (AttributeValueEntry entry : old_tuple)
            {
                Value old_value = entry.getValue();
                new_tuple.setAttribute(entry.getName(), cloneManual(old_value));
            }
            return (T) new_tuple;
        }
        else if (value instanceof CollectionValue)
        {
            CollectionValue old_relation = (CollectionValue) value;
            CollectionValue new_relation = new CollectionValue();
            for (TupleValue old_tuple : old_relation.getTuples())
            {
                new_relation.add(cloneManual(old_tuple));
            }
            return (T) new_relation;
        }
        else if (value instanceof SwitchValue)
        {
            SwitchValue old_switch_value = (SwitchValue) value;
            Value new_case = cloneManual(old_switch_value.getCase());
            return (T) new SwitchValue(old_switch_value.getCaseName(), new_case);
        }
        else if (value instanceof StringValue)
        {
            return (T) new StringValue(((StringValue) value).toString());
        }
        else if (value instanceof IntegerValue)
        {
            return (T) new IntegerValue(((IntegerValue) value).getObject());
        }
        else if (value instanceof DoubleValue)
        {
            return (T) new DoubleValue(((DoubleValue) value).getObject());
        }
        else if (value instanceof FloatValue)
        {
            return (T) new FloatValue(((FloatValue) value).getObject());
        }
        else if (value instanceof LongValue)
        {
            return (T) new LongValue(((LongValue) value).getObject());
        }
        else if (value instanceof DecimalValue)
        {
            return (T) new DecimalValue(((DecimalValue) value).getObject());
        }
        else if (value instanceof TimestampValue)
        {
            long millisecs = ((TimestampValue) value).getObject().getTime();
            return (T) new TimestampValue(new Timestamp(millisecs));
        }
        else if (value instanceof DateValue)
        {
            long millisecs = ((DateValue) value).getObject().getTime();
            return (T) new DateValue(new Date(millisecs));
        }
        else if (value instanceof BooleanValue)
        {
            return (T) new BooleanValue(((BooleanValue) value).getObject());
        }
        else if (value instanceof XhtmlValue)
        {
            String xhtml_str = ((XhtmlValue) value).toString();
            return (T) new XhtmlValue(xhtml_str);
        }
        else if (value instanceof JsonValue)
        {
            JsonValue json_value = (JsonValue) value;
            return (T) JsonValue.parse(json_value.toString());
        }
        else
        {
            assert (value instanceof NullValue);
            return (T) new NullValue(value.getTypeClass());
        }
    }
}
