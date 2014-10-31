/**
 * 
 */
package edu.ucsd.forward.experiment.memory_size;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.nio.ByteBuffer;

import net.sf.ehcache.pool.Size;
import net.sf.ehcache.pool.sizeof.AgentSizeOf;

import org.testng.annotations.Test;

import com.google.gwt.xml.client.Element;

import edu.ucsd.app2you.util.IoUtil;
import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.type.CollectionType;
import edu.ucsd.forward.data.type.IntegerType;
import edu.ucsd.forward.data.type.SchemaTree;
import edu.ucsd.forward.data.type.StringType;
import edu.ucsd.forward.data.type.TupleType;
import edu.ucsd.forward.data.type.Type;
import edu.ucsd.forward.data.value.CollectionValue;
import edu.ucsd.forward.data.value.DataTree;
import edu.ucsd.forward.data.value.IntegerValue;
import edu.ucsd.forward.data.value.StringValue;
import edu.ucsd.forward.data.value.TupleValue;
import edu.ucsd.forward.data.xml.TypeXmlParser;
import edu.ucsd.forward.data.xml.ValueXmlParser;
import edu.ucsd.forward.test.AbstractTestCase;
import edu.ucsd.forward.xml.XmlParserException;
import edu.ucsd.forward.xml.XmlUtil;

/**
 * Tests the memory usage by SQL++ type object graph.
 * @author GauravSaxena
 *
 */
@Test(groups = edu.ucsd.forward.test.AbstractTestCase.PERFORMANCE)
public class TestSqlppDataModelMemorySize extends AbstractTestCase
{
    private static final Logger log = Logger.getLogger(TestSqlppDataModelMemorySize.class);
    
    /**
     * Compares Flat structure to the minimum possible size.
     * @throws XmlParserException 
     * @throws FileNotFoundException 
     */
    @Test
    public void testMemoryFlat() throws XmlParserException, FileNotFoundException
    {
        int tuple_number = 10;
        long size_raw_data = size(getRawDataFlat(tuple_number));
        long size_sql_relation = size(getJavaClassFlat(tuple_number));
        long size_sqlpp_relation = size(getSqlppRelationFlat(tuple_number));
        log.info("\n-----Experiment 1: Flat-----\nRaw Data: " + size_raw_data + "\nClass Holding Data: " 
                + size_sql_relation + "\nSQL++ Data: " + size_sqlpp_relation);
    }
    
   
    /**
     * Creates SQL++ relation.
     * @param tuple_number number of tuples.
     * @return SQL++ Datatree.
     */
    private DataTree getSqlppRelationFlat(int tuple_number)
    {
        String key = "key";
        String user_id = "user_id";
        String name = "name";
        //schema
        CollectionType collection_type = new CollectionType();
        TupleType tuple_type = new TupleType();
        collection_type.setChildrenType(tuple_type);
        tuple_type.setAttribute(key, new IntegerType());
        tuple_type.setAttribute(user_id, new StringType());
        tuple_type.setAttribute(name, new StringType());
        //data
        CollectionValue root_value = new CollectionValue();
        root_value.setType(collection_type);
        DataTree data_tree = new DataTree(root_value);
        SchemaTree schema_tree = new SchemaTree(root_value.getType());
        data_tree.setType(schema_tree);
        for(int i = 0; i <tuple_number; i++) 
        {
            TupleValue tuple_value = new TupleValue();
            tuple_value.setAttribute(key, new IntegerValue(i));
            tuple_value.setAttribute(user_id, new StringValue("john@doe.com"));
            tuple_value.setAttribute(name, new StringValue("John Doe"));
            root_value.add(tuple_value);
        }
        return data_tree;
    }


    /**
     * Returns data as bytes.
     * @param tuple_number number of tuples.
     * @return byte array containing data
     */
    private byte[] getRawDataFlat(int tuple_number)
    {
        int row_length = 4 + "john@doe.com".length() + "John Doe".length();
        int schema_length = "key".length() + "user_id".length() + "name".length();
        ByteBuffer buffer = ByteBuffer.allocate(schema_length + tuple_number * row_length);
        buffer.put("key".getBytes());
        buffer.put("user_id".getBytes());
        buffer.put("name".getBytes());
        for(int i = 0; i < tuple_number; i++)
        {
            buffer.putInt(i);
            buffer.put("john@doe.com".getBytes());
            buffer.put("John Doe".getBytes());
        }
        return buffer.array();
    }

    /**
     * Returns size of an object.
     * @param obj object
     * @return size of obj
     */
    private long size(Object obj)
    {
        AgentSizeOf agent_size_of = new AgentSizeOf();
        Size size = agent_size_of.deepSizeOf(Integer.MAX_VALUE, false, obj);
        return size.getCalculated();
    }
    /**
     * Returns a relational table.
     * @return relational Table
     */
    private class PersonInfo 
    {
        private int key;
        private String user_id;
        private String name;
        /**
         * Constructor.
         * @param key1
         * @param user_id1
         * @param name1
         */
        public PersonInfo(int key1, String user_id1, String name1)
        {
            this.key = key1;
            this.user_id = user_id1;
            this.name = name1;
        }
    }
    /**
     * Creates class structure for a relational table.
     * @param tuple_number number of tuples.
     * @return table.
     */
    private Object[] getJavaClassFlat(int tuple_number)
    {
        String[] schema = new String[]{"key", "user_id", "name"};
        PersonInfo[] table = new PersonInfo[tuple_number];
        for(int i = 0; i < tuple_number; i++)
            table[i] = new PersonInfo(1, "john@doe.com", "John Doe");
        return new Object[]{schema, table};
    }
    /**
     * testMemoryNesting.
     * @throws FileNotFoundException 
     * @throws XmlParserException 
     */
    @Test
    public void testMemoryNesting() throws FileNotFoundException, XmlParserException
    {
        int tuple_number = 10;
        int nested_tuples = tuple_number / 10;
        long size_raw_data = size(getRawDataNested(tuple_number, nested_tuples));
        long size_class_data = size(getJavaClassNested(tuple_number, nested_tuples));
        long size_sqlpp_relation = size(getSqlppRelationNested(tuple_number, nested_tuples));
//        String source = "/home/gaurav/workspaces/DBLab/src/sketch/src/test/resources/edu/ucsd/forward/data/TestMemoryNestedDataModel-testFind.xml";
//        FileInputStream fis = new FileInputStream(new File(source));
//        Element source_elem = (Element) XmlUtil.parseDomNode(IoUtil.getInputStreamAsString(fis));
//        SchemaTree source_schema = TypeXmlParser.parseSchemaTree(XmlUtil.getOnlyChildElement(source_elem, "schema_tree"));
//        DataTree data_tree = ValueXmlParser.parseDataTree(XmlUtil.getOnlyChildElement(source_elem, "data_tree"), source_schema);
//        long size_sqlpp_relation = size(data_tree);
        log.info("\n-----Experiment 2: Nested-----\nRaw Data: " + size_raw_data 
                 + "\nClass Holding Data: " + size_class_data + "\nSQL++ Data: " + size_sqlpp_relation);
    }
    /**
     * Returns nested byte array.
     * @param tuple_number number of tuples.
     * @param nested_tuples number of nested tuples.
     * @return byte array.
     */
    private byte[] getRawDataNested(int tuple_number, int nested_tuples)
    {
        int row_length = 4 + "john@doe.com".length() + "John Doe".length() + nested_tuples * 4;
        int schema_length = "key".length() + "user_id".length() + "name".length();
        ByteBuffer buffer = ByteBuffer.allocate(schema_length + tuple_number * row_length);
        buffer.put("key".getBytes());
        buffer.put("user_id".getBytes());
        buffer.put("name".getBytes());
        for(int i = 0; i < tuple_number; i++)
        {
            buffer.putInt(i);
            buffer.put("john@doe.com".getBytes());
            buffer.put("John Doe".getBytes());
            for(int j = 0; j < nested_tuples; j++)
                buffer.putInt(999999991);
        }
        return buffer.array();
    }
    /**
     * Creates SQL++ nested relation.
     * @param tuple_number number of tuples.
     * @return SQL++ Datatree.
     */
    private DataTree getSqlppRelationNested(int tuple_number, int nested_tuples)
    {
        String key = "key";
        String user_id = "user_id";
        String name = "name";
        String phone = "phone";
        //schema
        CollectionType collection_type = new CollectionType();
        TupleType tuple_type = new TupleType();
        collection_type.setChildrenType(tuple_type);
        tuple_type.setAttribute(key, new IntegerType());
        tuple_type.setAttribute(user_id, new StringType());
        tuple_type.setAttribute(name, new StringType());
        CollectionType nested_collection_type = new CollectionType();
        tuple_type.setAttribute(name, nested_collection_type);
        nested_collection_type.setChildrenType(new IntegerType());
        //data
        CollectionValue root_value = new CollectionValue();
        root_value.setType(collection_type);
        DataTree data_tree = new DataTree(root_value);
        SchemaTree schema_tree = new SchemaTree(root_value.getType());
        data_tree.setType(schema_tree);
        for(int i = 0; i <tuple_number; i++) 
        {
            TupleValue tuple_value = new TupleValue();
            tuple_value.setAttribute(key, new IntegerValue(i));
            tuple_value.setAttribute(user_id, new StringValue("john@doe.com"));
            tuple_value.setAttribute(name, new StringValue("John Doe"));
            CollectionValue phone_numbers = new CollectionValue();
            tuple_value.setAttribute(phone, phone_numbers);
            for(int j = 0; j < nested_tuples; j++)
                phone_numbers.add(new IntegerValue(999999991));
            root_value.add(tuple_value);
        }
        return data_tree;
    }
    /**
     * Returns a relational table.
     * @return relational Table
     */
    private class PersonInfoNested 
    {
        private int key;
        private String user_id;
        private String name;
        private int[] phone;
        /**
         * Constructor.
         * @param key1
         * @param user_id1
         * @param name1
         * @param phone1
         */
        public PersonInfoNested(int key1, String user_id1, String name1, int[] phone1)
        {
            this.key = key1;
            this.user_id = user_id1;
            this.name = name1;
            this.phone = phone1;
        }
    }
    /**
     * Creates class structure for a relational table.
     * @param tuple_number number of tuples.
     * @return table.
     */
    private Object[] getJavaClassNested(int tuple_number, int nested_tuples)
    {
        String[] schema = new String[]{"key", "user_id", "name"};
        PersonInfoNested[] table = new PersonInfoNested[tuple_number];
        for(int i = 0; i < tuple_number; i++) 
        {
            table[i] = new PersonInfoNested(1, "john@doe.com", "John Doe", new int[nested_tuples]);
            for(int j = 0; j < nested_tuples; j++)
                table[i].phone[j] = 999999991;
        }
        return new Object[]{schema, table};
    }
}