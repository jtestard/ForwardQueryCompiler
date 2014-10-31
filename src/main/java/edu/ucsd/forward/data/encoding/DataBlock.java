package edu.ucsd.forward.data.encoding;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Set;
import java.util.Stack;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.type.BooleanType;
import edu.ucsd.forward.data.type.CollectionType;
import edu.ucsd.forward.data.type.DoubleType;
import edu.ucsd.forward.data.type.FloatType;
import edu.ucsd.forward.data.type.IntegerType;
import edu.ucsd.forward.data.type.LongType;
import edu.ucsd.forward.data.type.StringType;
import edu.ucsd.forward.data.type.TupleType;
import edu.ucsd.forward.data.type.Type;
import edu.ucsd.forward.data.value.BooleanValue;
import edu.ucsd.forward.data.value.CollectionValue;
import edu.ucsd.forward.data.value.DoubleValue;
import edu.ucsd.forward.data.value.FloatValue;
import edu.ucsd.forward.data.value.IntegerValue;
import edu.ucsd.forward.data.value.LongValue;
import edu.ucsd.forward.data.value.StringValue;
import edu.ucsd.forward.data.value.TupleValue;
import edu.ucsd.forward.data.value.Value;

/**
 * This class provides the put methods to encode and store values on the block. It also provides get methods to get decoded values
 * from the block.
 * 
 * @author Aditya Avinash
 * 
 *         TODO: 1. Implement block overflow logic. 2. Implement near pointer and far pointer 3. Analyze implementation for ANY and
 *         UNION types 4. Implement get methods
 * 
 */
public class DataBlock extends Block
{
    @SuppressWarnings("unused")
    private static final Logger  log                  = Logger.getLogger(DataBlock.class);
    
    private Stack<Integer>       m_position_stack     = new Stack<Integer>();
    
    private Stack<Integer>       m_value_count_stack  = new Stack<Integer>();
    
    private int                  m_value_id           = 0;
    
    private static final boolean DYNAMIC_TYPE_VALUE   = true;
    
    private static final boolean STATIC_TYPE_VALUE    = false;
    
    private static final int     NO_OVERFLOW          = 0;
    
    private static final int     OVERFLOW             = 1;
    
    private static final int     INITIAL_BYTE_LEN     = 0;
    
    private static final int     INITIAL_NO_OF_VALUES = 0;
    
    private static final boolean NOT_ORDERED          = false;
    
    private static final boolean ORDERED              = true;
    
    /**
     * We need to set the value count to be zero in the beginning. Example: Zero will be stored in stack initially and after storing
     * A bag of tuples, the final count 1 will remain in the stack.
     */
    public DataBlock()
    {
        m_value_count_stack.push(INITIAL_NO_OF_VALUES);
    }
    
    /**
     * It returns an integer value which is unique for a DataBlock
     */
    public int getUniqueValueID()
    {
        return m_value_id++;
    }
    
    /**
     * This method puts scheme ref, type ref at the beginning of the data block. it also sets the number of values on the block to
     * be 0
     * 
     * @param schema_block_ref
     * @param type_ref
     * @throws IOException
     */
    public void putTypeHeader(int schema_block_ref, int type_ref) throws IOException
    {
        if (m_write_position != 0)
        {
            throw new EncodingViolationException("Type header should be written only at the beginning of the data block");
        }
        putUnInt32(schema_block_ref);
        putUnInt32(type_ref);
        putFixedSizeInt(INITIAL_NO_OF_VALUES);
    }
    
    /**
     * When this method is called, number of values stored on the block is increased by one and new value id is stored on the block
     * 
     * @throws IOException
     */
    public int generateValueIdForNewValueToBeStored() throws IOException
    {
        increaseNumberOfValuesStored();
        int valueId = getUniqueValueID();
        putUnInt32(valueId);
        return valueId;
    }
    
    /**
     * This method does not change the m_read_postion and m_write_posotion variables
     * 
     * @throws IOException
     */
    private void increaseNumberOfValuesStored() throws IOException
    {
        int integer = readRawVarint32(0, ABSOLUTE_ACCESS);
        int position = computeRawVarint32Size(integer);
        integer = readRawVarint32(position, ABSOLUTE_ACCESS);
        position = position + computeRawVarint32Size(integer);
        int numberOfValues = getFixedSizeInt(position);
        putFixedSizeInt(position, numberOfValues + 1);
    }
    
    /*
     * =============================================================================================================================
     * Bulk put methods
     * =============================================================================================================================
     */
    public int putValue(Value value) throws IOException
    {
        int valueId = generateValueIdForNewValueToBeStored();
        storeValue(value);
        return valueId;
    }
    
    public void putArray(Value value) throws IOException
    {
        putArrayStart();
        for (Value val : value.getChildren())
        {
            storeValue(val);
        }
        putArrayEnd();
    }
    
    public void putBag(Value value) throws IOException
    {
        putBagStart();
        for (Value val : value.getChildren())
        {
            storeValue(val);
        }
        putBagEnd();
    }
    
    public void putTuple(Value value) throws IOException
    {
        putDenseTupleValue(value);
    }
    
    /**
     * @param value
     */
    public void putDenseTupleValue(Value value) throws IOException
    {
        putDenseTupleStart();
        Iterator<TupleValue.AttributeValueEntry> it = ((TupleValue) value).iterator();
        while (it.hasNext())
        {
            TupleValue.AttributeValueEntry entry = it.next();
            storeValue(entry.getValue());
        }
        putDenseTupleEnd();
    }
    
    /*
     * =========================================================================================================================
     * Incremental put methods
     * ========================================================================================================================
     */
    
    public void putArrayStart() throws IOException
    {
        putBagOrArrayStart();
    }
    
    public void putArrayEnd() throws IOException
    {
        updateByteLengthAndValueCount();
    }
    
    public void putBagStart() throws IOException
    {
        putBagOrArrayStart();
    }
    
    public void putBagEnd() throws IOException
    {
        updateByteLengthAndValueCount();
    }
    
    private void putBagOrArrayStart() throws IOException
    {
        putUnInt32(NO_OVERFLOW);
        increaseValueCountInStackAndPutNewValueCount();
        m_position_stack.push(m_write_position);
        putFixedSizeInt(INITIAL_BYTE_LEN);
        putFixedSizeInt(INITIAL_NO_OF_VALUES);
    }
    
    private void increaseValueCountInStackAndPutNewValueCount()
    {
        increaseValueCountInStack();
        m_value_count_stack.push(INITIAL_NO_OF_VALUES);
    }
    
    private void increaseValueCountInStack()
    {
        m_value_count_stack.push(m_value_count_stack.pop() + 1); // existingCount = m_value_count_stack.pop();
    }
    
    public void putEmptyTupleStart() throws IOException
    {
        increaseValueCountInStack();
        putUnInt32(ValueCode.EMPTY_TUPLE_VALUE.value());
    }
    
    public void putEmptyTupleEnd()
    {
        // No action needed.
    }
    
    public void putDenseTupleStart() throws IOException
    {
        putUnInt32(ValueCode.DENSE_TUPLE_VALUE.value());
        increaseValueCountInStackAndPutNewValueCount();
        m_position_stack.push(m_write_position);
        putFixedSizeInt(INITIAL_BYTE_LEN);
    }
    
    public void putDenseTupleEnd() throws IOException
    {
        updateByteLength();
        m_value_count_stack.pop();// Dense tuple keeps the number of values stored, but does not use it in encoding. Needed due to
        // stack implementation.
        
    }
    
    public void putSparseTupleStart() throws IOException
    {
        putUnInt32(ValueCode.SPARSE_TUPLE_VALUE.value());
        increaseValueCountInStackAndPutNewValueCount();
        m_position_stack.push(m_write_position);
        putFixedSizeInt(INITIAL_BYTE_LEN);
        putFixedSizeInt(INITIAL_NO_OF_VALUES);
    }
    
    public void putSparseTupleEnd() throws IOException
    {
        updateByteLengthAndValueCount();
    }
    
    public void putMappingTupleStart() throws IOException
    {
        putUnInt32(ValueCode.MAPPING_TUPLE_VALUE.value());
        increaseValueCountInStackAndPutNewValueCount();
        m_position_stack.push(m_write_position);
        putFixedSizeInt(INITIAL_BYTE_LEN);
        putFixedSizeInt(INITIAL_NO_OF_VALUES);
    }
    
    public void putMappingTupleEnd() throws IOException
    {
        updateByteLengthAndValueCount();
    }
    
    /**
     * For mapping tuple value, a pair of attribute_name and value makes one value so while storing attribute name we should not
     * increase the count in the value count stack.Hence we are calling put method of super class.
     * 
     * @param val
     * @throws IOException
     */
    public void putMappingTupleAtrributeName(String val) throws IOException
    {
        super.putString(val);
    }
    
    /**
     * For sparse tuple value, a pair of attribute_postion and value makes one value so while storing attribute position we should
     * not increase the count in the value count stack.Hence we are calling put method of super class.
     * 
     * @param val
     * @throws IOException
     */
    public void putSparseTupleAtrributePosition(int val) throws IOException
    {
        super.putUnInt32(val);
    }
    
    private void updateByteLength() throws IOException
    {
        int startPosition = m_position_stack.pop();
        putFixedSizeInt(startPosition, m_write_position - startPosition - 4); // byte_length = m_write_position - startPosition - 4;
    }
    
    private void updateByteLengthAndValueCount() throws IOException
    {
        int startPosition = m_position_stack.pop();
        putFixedSizeInt(startPosition, m_write_position - startPosition - 4); // byte_length = m_write_position - startPosition - 4;
        putFixedSizeInt(startPosition + 4, m_value_count_stack.pop()); // num of values will be at the top of value count stack
    }
    
    public void putDynamicTypedValueStart() throws IOException
    {
        putBoolean(DYNAMIC_TYPE_VALUE);
    }
    
    public void putDynamicTypedValueEnd()
    {
        // nothing needs to be done once the dynamic typed value ends
    }
    
    public void putStaticTypedValueStart() throws IOException
    {
        putBoolean(STATIC_TYPE_VALUE);
        
    }
    
    public void putStaticTypedValueEnd()
    {
        // nothing needs to be done once the static typed value ends
    }
    
    public void putFarPointer()
    {
        // TODO
    }
    
    public void putNearPointer()
    {
        // TODO
    }
    
    public void putDynamicTypedBooleanValue(boolean val) throws IOException
    {
        putUnInt32(TypeCode.BOOLEAN_TYPE.value());
        increaseValueCountInStack();
        super.putBoolean(val);
    }
    
    public void putDynamicTypedStringValue(String val) throws IOException
    {
        putUnInt32(TypeCode.STRING_TYPE.value());
        increaseValueCountInStack();
        super.putString(val);
    }
    
    public void putDynamicTypedIntValue(int val) throws IOException
    {
        putUnInt32(TypeCode.INTEGER_TYPE.value());
        increaseValueCountInStack();
        super.putInt32(val);
    }
    
    public void putDynamicTypedLongValue(long val) throws IOException
    {
        putUnInt32(TypeCode.LONG_TYPE.value());
        increaseValueCountInStack();
        super.putInt64(val);
    }
    
    public void putDynamicTypedFloatValue(float val) throws IOException
    {
        putUnInt32(TypeCode.FLOAT_TYPE.value());
        increaseValueCountInStack();
        super.putFloat(val);
    }
    
    public void putDynamicTypedDoubleValue(double val) throws IOException
    {
        putUnInt32(TypeCode.DOUBLE_TYPE.value());
        increaseValueCountInStack();
        super.putDouble(val);
    }
    
    @Override
    public void putBoolean(boolean val) throws IOException
    {
        putUnInt32(TypeCode.BOOLEAN_TYPE.value());
        increaseValueCountInStack();
        super.putBoolean(val);
    }
    
    @Override
    public void putString(String val) throws IOException
    {
        increaseValueCountInStack();
        super.putString(val);
    }
    
    @Override
    public void putInt32(int val) throws IOException
    {
        increaseValueCountInStack();
        super.putInt32(val);
    }
    
    @Override
    public void putInt64(long val) throws IOException
    {
        increaseValueCountInStack();
        super.putInt64(val);
    }
    
    @Override
    public void putFloat(float val) throws IOException
    {
        increaseValueCountInStack();
        super.putFloat(val);
    }
    
    @Override
    public void putDouble(double val) throws IOException
    {
        increaseValueCountInStack();
        super.putDouble(val);
    }
    
    /*
     * =============================================================================================================================
     * Bulk get methods
     * =============================================================================================================================
     */
    
    public Collection<Value> getAllValues() throws IOException
    {
        
        Collection<Value> values = new LinkedList<Value>();
        Type type = getTypeForBlock();
        int valueCount = getNumberOfValuesOnDataBlcok();
        moveReadPositionToFirstValueOnDataBlcok();
        for (int i = 0; i < valueCount; i++)
        {
            getUnInt32(); // value id
            values.add(getValue(type));
        }
        return values;
    }
    
    /**
     * It does not change the read position
     * 
     * @return
     * @throws IOException
     */
    public Type getTypeForBlock() throws IOException
    {
        int intialPosition = m_read_position;
        setReadPosition(0);
        int schemaBlockRef = getUnInt32();
        int typeRef = getUnInt32();
        SchemaBlock schemaBlock = (SchemaBlock) BlockDirectory.getBlock(schemaBlockRef);
        setReadPosition(intialPosition);
        return schemaBlock.getType(typeRef);
    }
    
    /**
     * It does not change the read position
     * 
     * @return
     * @throws IOException
     */
    public int getNumberOfValuesOnDataBlcok() throws IOException
    {
        int intialPosition = m_read_position;
        setReadPosition(0);
        getUnInt32();
        getUnInt32();
        int numOfvalues = getFixedSizeInt();
        setReadPosition(intialPosition);
        return numOfvalues;
    }
    
    public void moveReadPositionToFirstValueOnDataBlcok() throws IOException
    {
        setReadPosition(0);
        getUnInt32();// block ref
        getUnInt32();// type_ref
        getFixedSizeInt();// number of values on the block
    }
    
    /**
     * @param value
     */
    private void storeValue(Value value) throws IOException
    {
        if (value == null) return;
        
        if ((value.getClass()).equals(CollectionValue.class))
        {
            if (((CollectionValue) value).isOrdered())
            {
                putArray(value);
            }
            else
            {
                putBag(value);
            }
        }
        else if ((value.getClass()).equals(TupleValue.class))
        {
            putTuple(value);
        }
        else if ((value.getClass()).equals(BooleanValue.class))
        {
            putBoolean(((BooleanValue) value).getObject());
        }
        else if ((value.getClass()).equals(IntegerValue.class))
        {
            putInt32(((IntegerValue) value).getObject());
        }
        else if ((value.getClass()).equals(LongValue.class))
        {
            putInt64(((LongValue) value).getObject());
        }
        else if ((value.getClass()).equals(FloatValue.class))
        {
            putFloat(((FloatValue) value).getObject());
        }
        else if ((value.getClass()).equals(DoubleValue.class))
        {
            putDouble(((DoubleValue) value).getObject());
        }
        else if ((value.getClass()).equals(StringValue.class))
        {
            putString(((StringValue) value).getObject());
        }
        /*
         * else if ((type.getClass()).equals(TimestampType.class)){
         * 
         * }else if((type.getClass()).equals(FAR_POINTER_TYPE.class)){ }else if((type.getClass()).equals(NEAR_POINTER_TYPE.class)){
         * }else if((type.getClass()).equals(UNION_TYPE.class)){ }else if((type.getClass()).equals(ANY_TYPE.class)){ }
         */
        
    }
    
    /**
     * TODO: Handle sparse tuple values and mapping tuple values
     * 
     * @return
     * @throws IOException
     */
    private Value getValue(Type type) throws IOException
    {
        if (type instanceof CollectionType && ((CollectionType) type).isOrdered())
        {
            return getArrayValue(type);
        }
        else if (type instanceof CollectionType && !((CollectionType) type).isOrdered())
        {
            return getBagValue(type);
        }
        else if (type instanceof TupleType)
        {
            return getDenseTupleValue(type);
        }
        else if (type instanceof StringType)
        {
            return getStringValue();
        }
        else if (type instanceof DoubleType)
        {
            return getDoubleValue();
        }
        else if (type instanceof IntegerType)
        {
            return getIntegerValue();
        }
        else if (type instanceof LongType)
        {
            return getLongValue();
        }
        else if (type instanceof FloatType)
        {
            return getFloatValue();
        }
        else if (type instanceof DoubleType)
        {
            return getDoubleValue();
        }
        else if (type instanceof BooleanType)
        {
            return getBooleanValue();
        }
        /*
         * else if (type instanceof EnrichedType) { // getEnrichedValue();// to be implemented } else if (type instanceof
         * StringType) { // getFarPointerValue();// to be implemented } else if (type instanceof StringType) { //
         * getNearPointerValue();// to be implemented } else if (type instanceof StringType) { // getAnyValue();// to be implemented
         * } else if (type instanceof StringType) { // getUnionValue();// to be implemented }
         */
        return null;
        
    }
    
    /**
     * @param type
     * @return
     */
    private Value getArrayValue(Type type) throws IOException
    {
        return getCollectionValue(type, ORDERED);
    }
    
    /**
     * @param type
     * @return
     */
    private Value getBagValue(Type type) throws IOException
    {
        return getCollectionValue(type, NOT_ORDERED);
    }
    
    private Value getCollectionValue(Type type, boolean isOrdered) throws IOException
    {
        CollectionValue value = new CollectionValue();
        value.setOrdered(isOrdered);
        Type childrenType = ((CollectionType) type).getChildrenType();
        int overflow = getUnInt32();
        if (overflow == OVERFLOW)
        {
            int overflowBlock = getUnInt32();
        }
        int byteLen = getFixedSizeInt();
        int numOfValues = getFixedSizeInt();
        for (int i = 0; i < numOfValues; i++)
        {
            value.add(getValue(childrenType));
        }
        return value;
    }
    
    private Value getDenseTupleValue(Type type) throws IOException
    {
       int i = getUnInt32();
       getFixedSizeInt();
       TupleValue val = new TupleValue();
       Set<String> attributeNames = ((TupleType)type).getAttributeNames();
       for(String attributeName : attributeNames){
           Value attributeValue=getValue(((TupleType)type).getAttribute(attributeName));
           val.setAttribute(attributeName, attributeValue);
       }
        return val;
    }
    
    /**
     * @return
     * @throws IOException 
     */
    private Value getBooleanValue() throws IOException
    {
        return new BooleanValue(getBoolean());
    }
    
    /**
     * @return TODO: Need to fix for exact type
     */
    private Value getNumberValue() throws IOException
    {
        return new DoubleValue(getDouble());
    }
    
    /**
     *  @return
     */
    private Value getIntegerValue() throws IOException
    {
        return new IntegerValue(getInt32());
    }
    
    /**
     *  @return 
     */
    private Value getLongValue() throws IOException
    {
        return new LongValue(getInt64());
    }
    
    /**
     *  @return
     */
    private Value getFloatValue() throws IOException
    {
        return new FloatValue(getFloat());
    }
    
    /**
     *  @return
     */
    private Value getDoubleValue() throws IOException
    {
        return new DoubleValue(getDouble());
    }
    
    
    /**
     * @return
     */
    private Value getStringValue() throws IOException
    {
        return new StringValue(getString());
    }
}
