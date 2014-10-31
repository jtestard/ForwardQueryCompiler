/**
 * 
 */
package edu.ucsd.forward.data.encoding;

import java.io.IOException;
import java.util.Collection;

import org.testng.annotations.Test;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.type.CollectionType;
import edu.ucsd.forward.data.type.DoubleType;
import edu.ucsd.forward.data.type.IntegerType;
import edu.ucsd.forward.data.type.StringType;
import edu.ucsd.forward.data.type.TupleType;
import edu.ucsd.forward.data.value.CollectionValue;
import edu.ucsd.forward.data.value.IntegerValue;
import edu.ucsd.forward.data.value.StringValue;
import edu.ucsd.forward.data.value.TupleValue;
import edu.ucsd.forward.data.value.Value;
import edu.ucsd.forward.test.AbstractTestCase;

/**
 * @author Aditya Avinash
 * 
 *         TODO: 1. Add test cases for dynamic typed values
 */
@Test
public class TestDataBlock extends AbstractTestCase
{
    @SuppressWarnings("unused")
    private static final Logger  log                   = Logger.getLogger(TestDataBlock.class);
    
    private static final String  TEST_ATTRIBUTE_NAME_1 = "name";
    
    private static final String  TEST_ATTRIBUTE_NAME_2 = "id";
    
    private static final String  TEST_ATTRIBUTE_NAME_3 = "email";
    
    private static final int     TEST_INTEGER_1        = 133232;
    
    private static final int     TEST_INTEGER_2        = 1999232;
    
    private static final String  TEST_STRING_1         = "TestString1";
    
    private static final String  TEST_STRING_2         = "TestString2";
    
    private static final boolean NOT_ORDERED           = false;
    
    private static final int     NO_OVERFLOW           = 0;
    
    public void testPutTypeHeader() throws IOException
    
    {
        DataBlock dataBlock = new DataBlock();
        dataBlock.putTypeHeader(TEST_INTEGER_1, TEST_INTEGER_2);
        
        dataBlock.setReadPosition(0);
        int blockRef = dataBlock.getUnInt32();
        int typeRef = dataBlock.getUnInt32();
        int intialNumberOfValues = dataBlock.getFixedSizeInt();
        assertTrue("Block ref encoded incorrectly", blockRef == TEST_INTEGER_1);
        assertTrue("Type ref encoded incorrectly", typeRef == TEST_INTEGER_2);
        assertTrue(" Intial number of values encoded incorrectly", intialNumberOfValues == 0);
        
    }
    
    public void TestgenerateValueIdForNewValueToBeStoredOnDataBlock() throws IOException
    {
        DataBlock dataBlock = new DataBlock();
        dataBlock.putTypeHeader(TEST_INTEGER_1, TEST_INTEGER_2);
        int numOfValuesStored = 3;
        int value_id = dataBlock.generateValueIdForNewValueToBeStored();
        dataBlock.generateValueIdForNewValueToBeStored();
        dataBlock.generateValueIdForNewValueToBeStored();
        dataBlock.setReadPosition(0);
        int blockRef = dataBlock.getUnInt32();
        int typeRef = dataBlock.getUnInt32();
        int NumberOfValues = dataBlock.getFixedSizeInt();
        assertTrue("value id encoded incorrectly", dataBlock.getUnInt32() == value_id);
        assertTrue("Block ref encoded incorrectly", blockRef == TEST_INTEGER_1);
        assertTrue("Type ref encoded incorrectly", typeRef == TEST_INTEGER_2);
        assertTrue("Number of values encoded incorrectly", NumberOfValues == numOfValuesStored);
    }
    
    public void testPutArrayStartAndEnd() throws IOException
    
    {
        DataBlock dataBlock = createDataBlockWithHeaderDetails();
        dataBlock.putArrayStart();
        dataBlock.putString(TEST_STRING_1);
        dataBlock.putString(TEST_STRING_2);
        dataBlock.putArrayEnd();
        moveReadPositionTotheFirstValue(dataBlock);
        int overflow = dataBlock.getUnInt32();
        int byteLenFound = dataBlock.getFixedSizeInt();
        int numOfValues = dataBlock.getFixedSizeInt();
        String value1 = dataBlock.getString();
        String value2 = dataBlock.getString();
        // 4 for number of values, one to store string len and one for each character
        int actualByteLen = 4 + TEST_STRING_1.length() + 1 + TEST_STRING_2.length() + 1;
        assertTrue("overflow info encoded incorrectly", overflow == NO_OVERFLOW);
        assertTrue("Byte len  incorrect", byteLenFound == actualByteLen);
        assertTrue("Number of values encoded incorrectly", numOfValues == 2);
        assertTrue(" String was not encoded correctly", TEST_STRING_1.equals(value1));
        assertTrue(" String was not encoded correctly", TEST_STRING_2.equals(value2));
        
    }
    
    public void testPutBagStartAndEnd() throws IOException
    
    {
        DataBlock dataBlock = createDataBlockWithHeaderDetails();
        dataBlock.putBagStart();
        dataBlock.putString(TEST_STRING_1);
        dataBlock.putString(TEST_STRING_2);
        dataBlock.putBagEnd();
        moveReadPositionTotheFirstValue(dataBlock);
        int overflow = dataBlock.getUnInt32();
        int byteLenFound = dataBlock.getFixedSizeInt();
        int numOfValues = dataBlock.getFixedSizeInt();
        String value1 = dataBlock.getString();
        String value2 = dataBlock.getString();
        // 4 for number of values, one to store string len and one for each character
        int actualByteLen = 4 + TEST_STRING_1.length() + 1 + TEST_STRING_2.length() + 1;
        assertTrue("overflow info encoded incorrectly", overflow == NO_OVERFLOW);
        assertTrue("Byte len  incorrect", byteLenFound == actualByteLen);
        assertTrue("Number of values encoded incorrectly", numOfValues == 2);
        assertTrue(" String was not encoded correctly", TEST_STRING_1.equals(value1));
        assertTrue(" String was not encoded correctly", TEST_STRING_2.equals(value2));
    }
    
    public void testPutDenseTupleStartAndEnd() throws IOException
    {
        DataBlock dataBlock = createDataBlockWithHeaderDetails();
        dataBlock.putDenseTupleStart();
        dataBlock.putString(TEST_STRING_1);
        dataBlock.putString(TEST_STRING_2);
        dataBlock.putDenseTupleEnd();
        moveReadPositionTotheFirstValue(dataBlock);
        assertTrue("Dense tuple value not found", dataBlock.getUnInt32() == ValueCode.DENSE_TUPLE_VALUE.value());
        int byteLenFound = dataBlock.getFixedSizeInt();
        String value1 = dataBlock.getString();
        String value2 = dataBlock.getString();
        // one to store string len and one for each character
        int actualByteLen = TEST_STRING_1.length() + 1 + TEST_STRING_2.length() + 1;
        assertTrue("Byte len  incorrect", byteLenFound == actualByteLen);
        assertTrue(" String was not encoded correctly", TEST_STRING_1.equals(value1));
        assertTrue(" String was not encoded correctly", TEST_STRING_2.equals(value2));
    }
    
    public void testPutSparseTupleStartAndEnd() throws IOException
    {
        DataBlock dataBlock = createDataBlockWithHeaderDetails();
        dataBlock.putSparseTupleStart();
        dataBlock.putSparseTupleAtrributePosition(1);
        dataBlock.putString(TEST_STRING_1);
        dataBlock.putSparseTupleAtrributePosition(10);
        dataBlock.putString(TEST_STRING_2);
        dataBlock.putSparseTupleEnd();
        moveReadPositionTotheFirstValue(dataBlock);
        assertTrue("Sparse tuple value not found", dataBlock.getUnInt32() == ValueCode.SPARSE_TUPLE_VALUE.value());
        int byteLenFound = dataBlock.getFixedSizeInt();
        int noOfValues = dataBlock.getFixedSizeInt();
        int position1 = dataBlock.getUnInt32();
        String value1 = dataBlock.getString();
        int position2 = dataBlock.getUnInt32();
        String value2 = dataBlock.getString();
        // 4 for number of values, 1 for position, 1 for string len , 1 per character
        int actualByteLen = 4 + 1 + 1 + TEST_STRING_1.length() + 1 + 1 + TEST_STRING_2.length();
        assertTrue("Byte len  incorrect", byteLenFound == actualByteLen);
        assertTrue("Number of values encoded incorrectly", noOfValues == 2);
        assertTrue("sparse tuple attribute position encoded incorrectly", position1 == 1);
        assertTrue(" String was not encoded correctly", TEST_STRING_1.equals(value1));
        assertTrue("sparse tuple attribute position encoded incorrectly", position2 == 10);
        assertTrue(" String was not encoded correctly", TEST_STRING_2.equals(value2));
    }
    
    public void testPutMappingTupleStartAndEnd() throws IOException
    {
        DataBlock dataBlock = createDataBlockWithHeaderDetails();
        dataBlock.putMappingTupleStart();
        dataBlock.putMappingTupleAtrributeName(TEST_ATTRIBUTE_NAME_1);
        dataBlock.putString(TEST_STRING_1);
        dataBlock.putMappingTupleAtrributeName(TEST_ATTRIBUTE_NAME_2);
        dataBlock.putString(TEST_STRING_2);
        dataBlock.putMappingTupleEnd();
        moveReadPositionTotheFirstValue(dataBlock);
        assertTrue("Mapping tuple value not found", dataBlock.getUnInt32() == ValueCode.MAPPING_TUPLE_VALUE.value());
        int byteLenFound = dataBlock.getFixedSizeInt();
        int noOfValues = dataBlock.getFixedSizeInt();
        String attributeName1 = dataBlock.getString();
        String value1 = dataBlock.getString();
        String attributeName2 = dataBlock.getString();
        String value2 = dataBlock.getString();
        // 4 for number of values, 1 for string len , 1 per character
        int actualByteLen = 4 + 1 + TEST_ATTRIBUTE_NAME_1.length() + 1 + TEST_STRING_1.length() + 1
                + TEST_ATTRIBUTE_NAME_2.length() + 1 + TEST_STRING_2.length();
        assertTrue("Byte len  incorrect", byteLenFound == actualByteLen);
        assertTrue("Number of values encoded incorrectly", noOfValues == 2);
        assertTrue(" String was not encoded correctly", TEST_ATTRIBUTE_NAME_1.equals(attributeName1));
        assertTrue(" String was not encoded correctly", TEST_STRING_1.equals(value1));
        assertTrue(" String was not encoded correctly", TEST_ATTRIBUTE_NAME_2.equals(attributeName2));
        assertTrue(" String was not encoded correctly", TEST_STRING_2.equals(value2));
    }
    
    private DataBlock createDataBlockWithHeaderDetails() throws IOException
    {
        DataBlock dataBlock = new DataBlock();
        dataBlock.putTypeHeader(TEST_INTEGER_1, TEST_INTEGER_2);
        dataBlock.generateValueIdForNewValueToBeStored();
        return dataBlock;
    }
    
    private void moveReadPositionTotheFirstValue(DataBlock dataBlock) throws IOException
    {
        dataBlock.setReadPosition(0);
        dataBlock.getUnInt32();// block ref
        dataBlock.getUnInt32(); // type ref
        dataBlock.getFixedSizeInt(); // No. of values on the block
        dataBlock.getUnInt32(); // value id
    }
    
    /**
     * 
     * 
     * @throws IOException
     */
    public void testDataEncodingByIndividualMethods() throws IOException
    {
        CollectionType collectionType = new CollectionType();
        collectionType.setOrdered(NOT_ORDERED);
        TupleType tupleType = new TupleType();
        tupleType.setAttribute(TEST_ATTRIBUTE_NAME_1, new StringType());
        tupleType.setAttribute(TEST_ATTRIBUTE_NAME_2, new IntegerType());
        tupleType.setAttribute(TEST_ATTRIBUTE_NAME_3, new StringType());
        collectionType.setChildrenType(tupleType);
        SchemaBlock schemaBlock = new SchemaBlock();
        int type_id = schemaBlock.getUniqueTypeID();
        schemaBlock.putType(type_id, collectionType);
        int schema_block_ref = schemaBlock.getBlockID();
        
        CollectionValue collectionValue = new CollectionValue();
        collectionValue.setOrdered(NOT_ORDERED);
        TupleValue tupleValue = new TupleValue();
        tupleValue.setAttribute(TEST_ATTRIBUTE_NAME_1, new StringValue("Aditya"));
        tupleValue.setAttribute(TEST_ATTRIBUTE_NAME_2, new IntegerValue(1));
        tupleValue.setAttribute(TEST_ATTRIBUTE_NAME_3, new StringValue("aavinash"));
        collectionValue.add((Value) tupleValue);
        tupleValue = new TupleValue();
        tupleValue.setAttribute(TEST_ATTRIBUTE_NAME_1, new StringValue("Gaurav"));
        tupleValue.setAttribute(TEST_ATTRIBUTE_NAME_2, new IntegerValue(2));
        tupleValue.setAttribute(TEST_ATTRIBUTE_NAME_3, new StringValue("gsaxena"));
        collectionValue.add((Value) tupleValue);
        
        DataBlock dataBlock = new DataBlock();
        dataBlock.putTypeHeader(schema_block_ref, type_id);
        int valueId = dataBlock.generateValueIdForNewValueToBeStored();
        dataBlock.putBagStart();
        dataBlock.putDenseTupleStart();
        dataBlock.putString("Aditya");
        dataBlock.putInt32(1);
        dataBlock.putString("aavinash");
        dataBlock.putDenseTupleEnd();
        dataBlock.putDenseTupleStart();
        dataBlock.putString("Gaurav");
        dataBlock.putInt32(2);
        dataBlock.putString("gsaxena");
        dataBlock.putDenseTupleEnd();
        dataBlock.putBagEnd();
        
        assertTrue("Schema block ref was wrongly encoded", dataBlock.getUnInt32() == schema_block_ref);
        assertTrue("Type ref was wrongly encoded", dataBlock.getUnInt32() == type_id);
        assertTrue("Inorrect number of values on the block", dataBlock.getFixedSizeInt() == 1);
        assertTrue("Inorrect value id stored on the block", dataBlock.getUnInt32() == valueId);
        assertTrue("Inorrect overflow info on the block", dataBlock.getUnInt32() == 0);
        dataBlock.getFixedSizeInt();// byte len in bag
        // dataBlock.getFixedSizeInt();// no. of values in bag
        assertTrue("Inorrect number of values in bag", dataBlock.getFixedSizeInt() == 2);
        assertTrue("Dense tuple value not found", dataBlock.getUnInt32() == ValueCode.DENSE_TUPLE_VALUE.value());
        dataBlock.getFixedSizeInt();// byte len in dense tuple
        assertTrue(TEST_ATTRIBUTE_NAME_1 + " was not encoded correctly", "Aditya".equals(dataBlock.getString()));
        assertTrue(TEST_ATTRIBUTE_NAME_2 + " was not encoded correctly", dataBlock.getInt32() == 1);
        assertTrue(TEST_ATTRIBUTE_NAME_3 + " was not encoded correctly", "aavinash".equals(dataBlock.getString()));
        assertTrue("Dense tuple value not found", dataBlock.getUnInt32() == ValueCode.DENSE_TUPLE_VALUE.value());
        dataBlock.getFixedSizeInt();// byte len in dense tuple
        assertTrue(TEST_ATTRIBUTE_NAME_1 + " was not encoded correctly", "Gaurav".equals(dataBlock.getString()));
        assertTrue(TEST_ATTRIBUTE_NAME_2 + " was not encoded correctly", dataBlock.getInt32() == 2);
        assertTrue(TEST_ATTRIBUTE_NAME_3 + " was not encoded correctly", "gsaxena".equals(dataBlock.getString()));
    }
    
    public void testDataEncodingByPutValueMethod() throws IOException
    
    {
        CollectionType collectionType = new CollectionType();
        collectionType.setOrdered(NOT_ORDERED);
        TupleType tupleType = new TupleType();
        tupleType.setAttribute(TEST_ATTRIBUTE_NAME_1, new StringType());
        tupleType.setAttribute(TEST_ATTRIBUTE_NAME_2, new IntegerType());
        tupleType.setAttribute(TEST_ATTRIBUTE_NAME_3, new StringType());
        collectionType.setChildrenType(tupleType);
        SchemaBlock schemaBlock = new SchemaBlock();
        int type_id = schemaBlock.getUniqueTypeID();
        schemaBlock.putType(type_id, collectionType);
        int schema_block_ref = schemaBlock.getBlockID();
        
        CollectionValue collectionValue = new CollectionValue();
        collectionValue.setOrdered(NOT_ORDERED);
        TupleValue tupleValue = new TupleValue();
        tupleValue.setAttribute(TEST_ATTRIBUTE_NAME_1, new StringValue("Aditya"));
        tupleValue.setAttribute(TEST_ATTRIBUTE_NAME_2, new IntegerValue(1));
        tupleValue.setAttribute(TEST_ATTRIBUTE_NAME_3, new StringValue("aavinash"));
        collectionValue.add((Value) tupleValue);
        tupleValue = new TupleValue();
        tupleValue.setAttribute(TEST_ATTRIBUTE_NAME_1, new StringValue("Gaurav"));
        tupleValue.setAttribute(TEST_ATTRIBUTE_NAME_2, new IntegerValue(2));
        tupleValue.setAttribute(TEST_ATTRIBUTE_NAME_3, new StringValue("gsaxena"));
        collectionValue.add((Value) tupleValue);
        
        DataBlock dataBlock = new DataBlock();
        dataBlock.putTypeHeader(schema_block_ref, type_id);
        int valueId = dataBlock.putValue(collectionValue);
        dataBlock.setReadPosition(0);
        
        assertTrue("Schema block ref was wrongly encoded", dataBlock.getUnInt32() == schema_block_ref);
        assertTrue("Type ref was wrongly encoded", dataBlock.getUnInt32() == type_id);
        assertTrue("Inorrect number of values on the block", dataBlock.getFixedSizeInt() == 1);
        assertTrue("Inorrect value id stored on the block", dataBlock.getUnInt32() == valueId);
        assertTrue("Inorrect overflow info on the block", dataBlock.getUnInt32() == 0);
        dataBlock.getFixedSizeInt();// byte len in bag
        // dataBlock.getFixedSizeInt();// no. of values in bag
        assertTrue("Inorrect number of values in bag", dataBlock.getFixedSizeInt() == 2);
        assertTrue("Dense tuple value not found", dataBlock.getUnInt32() == ValueCode.DENSE_TUPLE_VALUE.value());
        dataBlock.getFixedSizeInt();// byte len in dense tuple
        assertTrue(TEST_ATTRIBUTE_NAME_1 + " was not encoded correctly", "Aditya".equals(dataBlock.getString()));
        assertTrue(TEST_ATTRIBUTE_NAME_2 + " was not encoded correctly", dataBlock.getInt32() == 1);
        assertTrue(TEST_ATTRIBUTE_NAME_3 + " was not encoded correctly", "aavinash".equals(dataBlock.getString()));
        assertTrue("Dense tuple value not found", dataBlock.getUnInt32() == ValueCode.DENSE_TUPLE_VALUE.value());
        dataBlock.getFixedSizeInt();// byte len in dense tuple
        assertTrue(TEST_ATTRIBUTE_NAME_1 + " was not encoded correctly", "Gaurav".equals(dataBlock.getString()));
        assertTrue(TEST_ATTRIBUTE_NAME_2 + " was not encoded correctly", dataBlock.getInt32() == 2);
        assertTrue(TEST_ATTRIBUTE_NAME_3 + " was not encoded correctly", "gsaxena".equals(dataBlock.getString()));
        
        int totalByteLenUsingPutValueMethod = dataBlock.m_write_position;
        
        dataBlock = new DataBlock();
        dataBlock.putTypeHeader(schema_block_ref, type_id);
        dataBlock.generateValueIdForNewValueToBeStored();
        dataBlock.putBagStart();
        dataBlock.putDenseTupleStart();
        dataBlock.putString("Aditya");
        dataBlock.putInt32(1);
        dataBlock.putString("aavinash");
        dataBlock.putDenseTupleEnd();
        dataBlock.putDenseTupleStart();
        dataBlock.putString("Gaurav");
        dataBlock.putInt32(2);
        dataBlock.putString("gsaxena");
        dataBlock.putDenseTupleEnd();
        dataBlock.putBagEnd();
        
        int totalByteLenUsingIndividualMethods = dataBlock.m_write_position;
        assertTrue("The len of encoding by putValue method is incorrect",
                   totalByteLenUsingPutValueMethod == totalByteLenUsingIndividualMethods);
    }
    
    public void testDataDecodeUsingGetAllValues() throws IOException
    
    {
        CollectionType collectionType = new CollectionType();
        collectionType.setOrdered(NOT_ORDERED);
        TupleType tupleType = new TupleType();
        tupleType.setAttribute(TEST_ATTRIBUTE_NAME_1, new StringType());
        tupleType.setAttribute(TEST_ATTRIBUTE_NAME_2, new IntegerType());
        tupleType.setAttribute(TEST_ATTRIBUTE_NAME_3, new StringType());
        collectionType.setChildrenType(tupleType);
        SchemaBlock schemaBlock = new SchemaBlock();
        int type_id = schemaBlock.getUniqueTypeID();
        schemaBlock.putType(type_id, collectionType);
        int schema_block_ref = schemaBlock.getBlockID();
        
        CollectionValue collectionValue = new CollectionValue();
        collectionValue.setOrdered(NOT_ORDERED);
        TupleValue tupleValue = new TupleValue();
        tupleValue.setAttribute(TEST_ATTRIBUTE_NAME_1, new StringValue("Aditya"));
        tupleValue.setAttribute(TEST_ATTRIBUTE_NAME_2, new IntegerValue(1));
        tupleValue.setAttribute(TEST_ATTRIBUTE_NAME_3, new StringValue("aavinash"));
        collectionValue.add((Value) tupleValue);
        tupleValue = new TupleValue();
        tupleValue.setAttribute(TEST_ATTRIBUTE_NAME_1, new StringValue("Gaurav"));
        tupleValue.setAttribute(TEST_ATTRIBUTE_NAME_2, new IntegerValue(2));
        tupleValue.setAttribute(TEST_ATTRIBUTE_NAME_3, new StringValue("gsaxena"));
        collectionValue.add((Value) tupleValue);
        
        DataBlock dataBlock = new DataBlock();
        dataBlock.putTypeHeader(schema_block_ref, type_id);
        int valueId = dataBlock.putValue(collectionValue);
        //Collection<Value> dataDecoded = dataBlock.getAllValues();
        dataBlock.setReadPosition(0);
        
        assertTrue("Schema block ref was wrongly encoded", dataBlock.getUnInt32() == schema_block_ref);
        assertTrue("Type ref was wrongly encoded", dataBlock.getUnInt32() == type_id);
        assertTrue("Inorrect number of values on the block", dataBlock.getFixedSizeInt() == 1);
        assertTrue("Inorrect value id stored on the block", dataBlock.getUnInt32() == valueId);
        assertTrue("Inorrect overflow info on the block", dataBlock.getUnInt32() == 0);
        dataBlock.getFixedSizeInt();// byte len in bag
        // dataBlock.getFixedSizeInt();// no. of values in bag
        assertTrue("Inorrect number of values in bag", dataBlock.getFixedSizeInt() == 2);
        assertTrue("Dense tuple value not found", dataBlock.getUnInt32() == ValueCode.DENSE_TUPLE_VALUE.value());
        dataBlock.getFixedSizeInt();// byte len in dense tuple
        assertTrue(TEST_ATTRIBUTE_NAME_1 + " was not encoded correctly", "Aditya".equals(dataBlock.getString()));
        assertTrue(TEST_ATTRIBUTE_NAME_2 + " was not encoded correctly", dataBlock.getInt32() == 1);
        assertTrue(TEST_ATTRIBUTE_NAME_3 + " was not encoded correctly", "aavinash".equals(dataBlock.getString()));
        assertTrue("Dense tuple value not found", dataBlock.getUnInt32() == ValueCode.DENSE_TUPLE_VALUE.value());
        dataBlock.getFixedSizeInt();// byte len in dense tuple
        assertTrue(TEST_ATTRIBUTE_NAME_1 + " was not encoded correctly", "Gaurav".equals(dataBlock.getString()));
        assertTrue(TEST_ATTRIBUTE_NAME_2 + " was not encoded correctly", dataBlock.getInt32() == 2);
        assertTrue(TEST_ATTRIBUTE_NAME_3 + " was not encoded correctly", "gsaxena".equals(dataBlock.getString()));
        
        int totalByteLenUsingPutValueMethod = dataBlock.m_write_position;
        
        dataBlock = new DataBlock();
        dataBlock.putTypeHeader(schema_block_ref, type_id);
        dataBlock.generateValueIdForNewValueToBeStored();
        dataBlock.putBagStart();
        dataBlock.putDenseTupleStart();
        dataBlock.putString("Aditya");
        dataBlock.putInt32(1);
        dataBlock.putString("aavinash");
        dataBlock.putDenseTupleEnd();
        dataBlock.putDenseTupleStart();
        dataBlock.putString("Gaurav");
        dataBlock.putInt32(2);
        dataBlock.putString("gsaxena");
        dataBlock.putDenseTupleEnd();
        dataBlock.putBagEnd();
        
        int totalByteLenUsingIndividualMethods = dataBlock.m_write_position;
        assertTrue("The len of encoding by putValue method is incorrect",
                   totalByteLenUsingPutValueMethod == totalByteLenUsingIndividualMethods);
    }
    
}
