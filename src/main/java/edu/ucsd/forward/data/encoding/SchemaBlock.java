/**
 * 
 */
package edu.ucsd.forward.data.encoding;

import java.io.IOException;
import java.util.Iterator;

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

;

/**
 * @author Aditya Avinash
 * 
 */
public class SchemaBlock extends Block
{
    @SuppressWarnings("unused")
    private static final Logger  log         = Logger.getLogger(SchemaBlock.class);
    
    private static final boolean ORDERED     = true;
    
    private static final boolean NOT_ORDERED = false;
    
    private int                  m_type_id   = 0;
    
    /**
     * It returns a integer value which is unique for a SchemaBlock
     */
    public int getUniqueTypeID()
    {
        return m_type_id++;
    }
    
    public void putType(int type_id, Type type) throws IOException
    {
        increaseNumberOfTypesStoredInBlock();
        storeTypeId(type_id);
        storeType(type);
    }
    
    public Type getType(int type_id) throws IOException
    {
        setReadPosition(0);
        int numOfTypes = getFixedSizeInt();
        for (int i = 0; i < numOfTypes; i++)
        {
            int type_id_fetched = getUnInt32();
            Type type = getType();
            if (type_id_fetched == type_id)
            {
                return type;
            }
        }
        return null;
    }
    
    /**
     * @return
     * @throws IOException
     */
    private Type getType() throws IOException
    {
        int code_val = getUnInt32();
        if (code_val == TypeCode.ARRAY_TYPE.value())
        {
            return getArrayType();
        }
        else if (code_val == TypeCode.BAG_TYPE.value())
        {
            return getBagType();
        }
        else if (code_val == TypeCode.TUPLE_TYPE.value())
        {
            return getTupleType();
        }
        else if (code_val == TypeCode.STRING_TYPE.value())
        {
            return getStringType();
        }
        else if (code_val == TypeCode.NUMBER_TYPE.value())
        {
            return getNumberType();
        }
        else if (code_val == TypeCode.INTEGER_TYPE.value())
        {
            return getIntegerType();
        }
        else if (code_val == TypeCode.LONG_TYPE.value())
        {
            return getLongType();
        }
        else if (code_val == TypeCode.FLOAT_TYPE.value())
        {
            return getFloatType();
        }
        else if (code_val == TypeCode.DOUBLE_TYPE.value())
        {
            return getDoubleType();
        }
        else if (code_val == TypeCode.BOOLEAN_TYPE.value())
        {
            return getBooleanType();
        }
        else if (code_val == TypeCode.ENRICHED_TYPE.value())
        {
            // getEnrichedType();// to be implemented
        }
        else if (code_val == TypeCode.FAR_POINTER_TYPE.value())
        {
            // getFarPointerType();// to be implemented
        }
        else if (code_val == TypeCode.NEAR_POINTER_TYPE.value())
        {
            // getNearPointertype();// to be implemented
        }
        else if (code_val == TypeCode.ANY_TYPE.value())
        {
            // getAnyType();// to be implemented
        }
        else if (code_val == TypeCode.UNION_TYPE.value())
        {
            // getUnionType();// to be implemented
        }
        return null;
        
    }
    
    /**
     * @return
     */
    private Type getBooleanType()
    {
        return new BooleanType();
    }
    
    /**
     * @return TODO: Need to fix for exact type
     */
    private Type getNumberType()
    {
        return new DoubleType();
    }
    
    /**
     *  @return
     */
    private Type getIntegerType()
    {
        return new IntegerType();
    }
    
    /**
     *  @return 
     */
    private Type getLongType()
    {
        return new LongType();
    }
    
    /**
     *  @return
     */
    private Type getFloatType()
    {
        return new FloatType();
    }
    
    /**
     *  @return
     */
    private Type getDoubleType()
    {
        return new DoubleType();
    }
    
    
    /**
     * @return
     */
    private Type getStringType()
    {
        return new StringType();
    }
    
    /**
     * @throws IOException
     * 
     */
    private Type getTupleType() throws IOException
    {
        // TODO: We need to store this value somewhere in TupleType
        boolean isOpen = getBoolean();
        int attributeCount = getUnInt32();
        TupleType tupleType = new TupleType();
        for (int i = 0; i < attributeCount; i++)
        {
            tupleType.setAttribute(getString(), getType());
        }
        return tupleType;
    }
    
    /**
     * @return
     * @throws IOException
     */
    private Type getBagType() throws IOException
    {
        return getCollectionType(NOT_ORDERED);
    }
    
    /**
     * @return
     * @throws IOException
     */
    private Type getArrayType() throws IOException
    {
        return getCollectionType(ORDERED);
    }
    
    /**
     * @return
     * @throws IOException
     */
    private Type getCollectionType(boolean isOrdered) throws IOException
    {
        CollectionType collectionType = new CollectionType();
        collectionType.setOrdered(isOrdered);
        collectionType.setChildrenType(getType());
        return collectionType;
    }
    
    private void storeType(Type type) throws IOException
    {
        TypeCode code = DataEncodingUtil.findType(type);
        if (code == TypeCode.ARRAY_TYPE)
        {
            storeArrayType(type);
        }
        else if (code == TypeCode.BAG_TYPE)
        {
            storeBagType(type);
        }
        else if (code == TypeCode.TUPLE_TYPE)
        {
            storeTupleType(type);
        }
        else if (code == TypeCode.STRING_TYPE)
        {
            storeStringType();
        }
        else if (code == TypeCode.NUMBER_TYPE)
        {
            storeNumberType(type);
        }
        else if (code == TypeCode.INTEGER_TYPE)
        {
            storeIntegerType(type);
        }
        else if (code == TypeCode.LONG_TYPE)
        {
            storeLongType(type);
        }
        else if (code == TypeCode.FLOAT_TYPE)
        {
            storeFloatType(type);
        }
        else if (code == TypeCode.DOUBLE_TYPE)
        {
            storeDoubleType(type);
        }
        else if (code == TypeCode.BOOLEAN_TYPE)
        {
            storeBooleanType(type);
        }
        else if (code == TypeCode.ENRICHED_TYPE)
        {
            // storeEnrichedType(type);
        }
        else if (code == TypeCode.FAR_POINTER_TYPE)
        {
            // storeFarPointerType(type);
        }
        else if (code == TypeCode.NEAR_POINTER_TYPE)
        {
            // storeNearPointertype(type);
        }
        else if (code == TypeCode.ANY_TYPE)
        {
            // storeAnyType(type);
        }
        else if (code == TypeCode.UNION_TYPE)
        {
            // storeUnionType(type);
        }
    }
    
    /**
     * @param type
     * @throws IOException
     */
    private void storeBooleanType(Type type) throws IOException
    {
        putUnInt32(TypeCode.BOOLEAN_TYPE.value());
        
    }
    
    /**
     * @param type
     */
    private void storeNumberType(Type type) throws IOException
    {
        putUnInt32(TypeCode.NUMBER_TYPE.value());
        
    }
    /**
     * @param type
     */
    private void storeIntegerType(Type type) throws IOException
    {
        putUnInt32(TypeCode.INTEGER_TYPE.value());
        
    }
    /**
     * @param type
     */
    private void storeLongType(Type type) throws IOException
    {
        putUnInt32(TypeCode.LONG_TYPE.value());
        
    }
    /**
     * @param type
     */
    private void storeFloatType(Type type) throws IOException
    {
        putUnInt32(TypeCode.FLOAT_TYPE.value());
        
    }
    /**
     * @param type
     */
    private void storeDoubleType(Type type) throws IOException
    {
        putUnInt32(TypeCode.DOUBLE_TYPE.value());
        
    }
    
    /**
     * @throws IOException
     * 
     */
    private void storeStringType() throws IOException
    {
        putUnInt32(TypeCode.STRING_TYPE.value());
        
    }
    
    /**
     * @param type
     * @throws IOException
     */
    private void storeTupleType(Type type) throws IOException
    {
        TupleType tupleType = (TupleType) type;
        putUnInt32(TypeCode.TUPLE_TYPE.value());
        // TODO: Need to get this info from TupleType.java
        boolean isOpen = false;
        putBoolean(isOpen);
        putUnInt32(tupleType.getSize());
        
        Iterator<TupleType.AttributeEntry> it = tupleType.iterator();
        while (it.hasNext())
        {
            TupleType.AttributeEntry entry = it.next();
            putString(entry.getName());
            storeType(entry.getType());
        }
    }
    
    private void storeBagType(Type type) throws IOException
    {
        putUnInt32(TypeCode.BAG_TYPE.value());
        storeType(((CollectionType) type).getChildrenType());
    }
    
    private void storeArrayType(Type type) throws IOException
    {
        putUnInt32(TypeCode.ARRAY_TYPE.value());
        storeType(((CollectionType) type).getChildrenType());
    }
    
    /**
     * @param type
     * @throws IOException
     */
    private void storeTypeId(int typeID) throws IOException
    {
        putUnInt32(typeID);
        
    }
    
    /**
     * @throws IOException
     *             When a new type is stored, we will increase the count which maintains the number of types stored in the schema
     *             block
     */
    private void increaseNumberOfTypesStoredInBlock() throws IOException
    {
        if (getWritePosition() == 0)
        {
            putFixedSizeInt(1);
        }
        else
        {
            int numberOfTypes = getFixedSizeInt(0);
            putFixedSizeInt(0, numberOfTypes + 1);
        }
        
    }
    
   
}
