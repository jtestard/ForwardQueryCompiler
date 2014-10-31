/**
 * 
 */
package edu.ucsd.forward.data.encoding;

import java.io.IOException;
import java.util.Collection;

import net.sf.ehcache.pool.Size;
import net.sf.ehcache.pool.sizeof.AgentSizeOf;

import org.testng.annotations.Test;
import org.w3c.dom.Element;

import edu.ucsd.app2you.util.IoUtil;
import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.type.CollectionType;
import edu.ucsd.forward.data.type.DoubleType;
import edu.ucsd.forward.data.type.IntegerType;
import edu.ucsd.forward.data.type.StringType;
import edu.ucsd.forward.data.type.TupleType;
import edu.ucsd.forward.data.type.Type;
import edu.ucsd.forward.data.value.CollectionValue;
import edu.ucsd.forward.data.value.IntegerValue;
import edu.ucsd.forward.data.value.StringValue;
import edu.ucsd.forward.data.value.TupleValue;
import edu.ucsd.forward.data.value.Value;
import edu.ucsd.forward.data.xml.TypeXmlParser;
import edu.ucsd.forward.data.xml.ValueXmlParser;
import edu.ucsd.forward.test.AbstractTestCase;
import edu.ucsd.forward.util.xml.XmlUtil;
import edu.ucsd.forward.xml.ElementProxy;
import edu.ucsd.forward.xml.XmlParserException;

/**
 * The purpose of this test class is used to measure the space efficiency achieved by using data model encoding
 * 
 * @author AdityaAvinash
 * 
 */
@Test(groups = edu.ucsd.forward.test.AbstractTestCase.PERFORMANCE)
public class TestDataModelEncodingSizeComparison extends AbstractTestCase
{
    @SuppressWarnings("unused")
    private static final Logger  log                                         = Logger.getLogger(TestDataModelEncodingSizeComparison.class);
    
    private static final String  TEST_ATTRIBUTE_NAME_1                       = "name";
    
    private static final String  TEST_ATTRIBUTE_NAME_2                       = "id";
    
    private static final String  TEST_ATTRIBUTE_NAME_3                       = "email";
    
    private static final boolean NOT_ORDERED                                 = false;
    
    private static final String  REAL_TYPE_AND_DATA_FROM_FORWARD_APPLICATION = "forward_data_for_testing_schema_and_data_encoding.xml";
    
    public void testSchemaEncodingSizeEfficiency() throws IOException
    
    {
        SchemaBlock schemaBlock = new SchemaBlock();
        long currentSchemaSize = 0;
        for (int i = 0; i < 1000; i++)
        {
            CollectionType collectionType = new CollectionType();
            collectionType.setOrdered(NOT_ORDERED);
            TupleType tupleType = new TupleType();
            tupleType.setAttribute(TEST_ATTRIBUTE_NAME_1, new StringType());
            tupleType.setAttribute(TEST_ATTRIBUTE_NAME_2, new IntegerType());
            tupleType.setAttribute(TEST_ATTRIBUTE_NAME_3, new StringType());
            collectionType.setChildrenType(tupleType);
            int typeID = schemaBlock.getUniqueTypeID();
            schemaBlock.putType(typeID, collectionType);
            schemaBlock.getType(typeID);
            currentSchemaSize += size(collectionType);
        }
        long encodedSchemaSize = schemaBlock.getWritePosition();
        System.out.println("old: " + currentSchemaSize + " Encoded: " + encodedSchemaSize + " Space efficiency ratio: "
                + (currentSchemaSize * 1.0) / encodedSchemaSize);
    }
    
    public void testDataEncodingSizeEfficiency() throws IOException
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
        
        for (int i = 0; i < 600; i++)
        {
            TupleValue tupleValue = new TupleValue();
            tupleValue.setAttribute(TEST_ATTRIBUTE_NAME_1, new StringValue("Aditya" + i));
            tupleValue.setAttribute(TEST_ATTRIBUTE_NAME_2, new IntegerValue(1));
            tupleValue.setAttribute(TEST_ATTRIBUTE_NAME_3, new StringValue("aavinash" + i));
            collectionValue.add((Value) tupleValue);
            tupleValue = new TupleValue();
            tupleValue.setAttribute(TEST_ATTRIBUTE_NAME_1, new StringValue("Gaurav" + i));
            tupleValue.setAttribute(TEST_ATTRIBUTE_NAME_2, new IntegerValue(2));
            tupleValue.setAttribute(TEST_ATTRIBUTE_NAME_3, new StringValue("gsaxena" + i));
            collectionValue.add((Value) tupleValue);
        }
        
        DataBlock dataBlock = new DataBlock();
        dataBlock.putTypeHeader(schema_block_ref, type_id);
        dataBlock.putValue(collectionValue);
        long valueSize = size(collectionValue);
        long encodedValueSize = dataBlock.getWritePosition();
        System.out.println("old: " + valueSize + " Encoded: " + encodedValueSize + " Space efficiency ratio: " + (valueSize * 1.0)
                / encodedValueSize);
    }
    
    /**
     * 
     * 
     * @throws IOException
     * @throws XmlParserException
     */
    public void testDataEncodingForRealFORWARDData() throws IOException, XmlParserException
    
    {
        String string_value = IoUtil.getResourceAsString(REAL_TYPE_AND_DATA_FROM_FORWARD_APPLICATION);
        Element root = (Element) XmlUtil.parseDomNode(string_value);
        ElementProxy type_element = ElementProxy.makeGwtElement((Element) XmlUtil.getChildElements(root).get(0));
        ElementProxy value_element = ElementProxy.makeGwtElement((Element) XmlUtil.getChildElements(root).get(1));
        
        CollectionType collectionType = new CollectionType();
        collectionType.setOrdered(NOT_ORDERED);
        TupleType type = (TupleType) TypeXmlParser.parseType(type_element);
        collectionType.setChildrenType(type);
        
        CollectionValue collectionValue = new CollectionValue();
        collectionValue.setOrdered(NOT_ORDERED);
        TupleValue value = (TupleValue) ValueXmlParser.parseValue(value_element, type);
        collectionValue.add((Value) value);
        
        SchemaBlock schemaBlock = new SchemaBlock();
        int type_id = schemaBlock.getUniqueTypeID();
        schemaBlock.putType(type_id, collectionType);
        Type originalType = collectionType;
        Type decodedType = schemaBlock.getType(type_id);
        assertTrue("Schema type decoded is incorrect", originalType.getClass().equals(decodedType.getClass()));
        assertTrue("Schema type decoded is incorrect", originalType.toString().equals(decodedType.toString()));
        
        DataBlock dataBlock = new DataBlock();
        dataBlock.putTypeHeader(schemaBlock.getBlockID(), type_id);
        dataBlock.putValue(collectionValue);
        
        long valueSize = size(collectionValue);
        long encodedValueSize = dataBlock.getWritePosition();
        //Collection<Value> dataDecoded = dataBlock.getAllValues();
        System.out.println("Space efficiency comparison of encoding with real FORWARD data");
        System.out.println("old: " + valueSize + " Encoded: " + encodedValueSize + " Space efficiency ratio for data: " + (valueSize * 1.0)
                / encodedValueSize);
        
        long typeSize = size(collectionType);
        long encodedTypeSize = schemaBlock.getWritePosition();
        System.out.println("old: " + typeSize + " Encoded: " + encodedTypeSize + " Space efficiency ratio for schema: " + (typeSize * 1.0)
                / encodedTypeSize);
    }
    
    /**
     * Returns size of an object.
     * 
     * @param obj
     *            object
     * @return size of obj
     */
    private long size(Object obj)
    {
        AgentSizeOf agent_size_of = new AgentSizeOf();
        Size size = agent_size_of.deepSizeOf(Integer.MAX_VALUE, false, obj);
        return size.getCalculated();
    }
}
