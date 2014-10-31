/**
 * 
 */
package edu.ucsd.forward.data.java;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.testng.annotations.Test;
import com.google.gwt.xml.client.Element;

import edu.ucsd.app2you.util.IoUtil;
import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.type.SchemaTree;
import edu.ucsd.forward.data.value.DataTree;
import edu.ucsd.forward.data.xml.TypeXmlParser;
import edu.ucsd.forward.data.xml.ValueXmlParser;
import edu.ucsd.forward.test.AbstractTestCase;
import edu.ucsd.forward.xml.XmlUtil;

/**
 * Tests the Java object to SQL++ value mapping.
 * 
 * @author Yupeng
 * 
 */
@Test
public class TestJavaToSqlMapping extends AbstractTestCase
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(TestJavaToSqlMapping.class);
    
    /**
     * A class used for testing mapping to tuple value.
     * 
     * @author Yupeng
     * 
     */
    class TupleClass
    {
        private int        m_integer;
        
        private String     m_string;
        
        private double     m_double;
        
        private long       m_long;
        
        private float      m_float;
        
        private BigDecimal m_decimal;
        
        private String     m_xhtml;
        
        private Boolean    m_boolean;
        
        private Date       m_date;
        
        private Timestamp  m_time;
        
        public TupleClass(int integer, String string, Double d, long l, float f, BigDecimal decimal, String xhtml, boolean b,
                Date date, Timestamp time)
        {
            m_integer = integer;
            m_string = string;
            m_double = d;
            m_long = l;
            m_float = f;
            m_decimal = decimal;
            m_xhtml = xhtml;
            m_boolean = b;
            m_date = date;
            m_time = time;
        }
        
        public int getInteger()
        {
            return m_integer;
        }
        
        public String getString()
        {
            return m_string;
        }
        
        public double getDouble()
        {
            return m_double;
        }
        
        public long getLong()
        {
            return m_long;
        }
        
        public float getFloat()
        {
            return m_float;
        }
        
        public String getXhtml()
        {
            return m_xhtml;
        }
        
        public boolean getBoolean()
        {
            return m_boolean;
        }
        
        public Date getDate()
        {
            return m_date;
        }
        
        public Timestamp getTime()
        {
            return m_time;
        }
        
        public BigDecimal getDecimal()
        {
            return m_decimal;
        }
    }
    
    /**
     * Tests mapping to tuple value.
     * 
     * @throws Exception
     *             if any error occurs.
     */
    public void testTuple() throws Exception
    {
        verify(
               "TestJavaToSqlMapping-testTuple-CustomClass",
               new TupleClass(
                              20,
                              "test",
                              2.0d,
                              10L,
                              1.5f,
                              new BigDecimal("0.1"),
                              new String("<xml></xml>"),
                              true,
                              new Date(DateFormat.getDateInstance(DateFormat.MEDIUM, Locale.US).parse("Nov 4, 2003").getTime()),
                              new Timestamp(
                                            DateFormat.getDateTimeInstance(DateFormat.MEDIUM, DateFormat.FULL, Locale.US).parse(
                                                                                                                     "Nov 4, 2003 11:02:03 PM PST").getTime())));
        Map<String, Object> map = new HashMap<String, Object>();
        map.put("string", "test");
        map.put("integer", 20);
        Map<String, Object> child_map = new HashMap<String, Object>();
        map.put("tuple", child_map);
        child_map.put("boolean", false);
        verify("TestJavaToSqlMapping-testTuple-Map", map);
    }
    
    /**
     * Tests mapping to switch value.
     * 
     * @throws Exception
     *             if any error occurs.
     */
    public void testSwitch() throws Exception
    {
        Map<String, Object> map = new HashMap<String, Object>();
        map.put("string", "test");
        verify("TestJavaToSqlMapping-testSwitch-Map", map);
    }
    
    class CollectionTuple
    {
        private int    m_integer;
        
        private String m_string;
        
        public CollectionTuple(String string, int integer)
        {
            m_integer = integer;
            m_string = string;
        }
        
        public int getInteger()
        {
            return m_integer;
        }
        
        public String getString()
        {
            return m_string;
        }
    }
    
    /**
     * Tests mapping to collection value.
     * 
     * @throws Exception
     *             if any error occurs.
     */
    public void testCollection() throws Exception
    {
        List<CollectionTuple> list = new ArrayList<CollectionTuple>();
        list.add(new CollectionTuple("t1", 10));
        list.add(new CollectionTuple("t2", 20));
        verify("TestJavaToSqlMapping-testCollection-List", list);
        Map<Integer, String> map = new HashMap<Integer, String>();
        map.put(1, "t1");
        map.put(2, "t2");
        verify("TestJavaToSqlMapping-testCollection-Map", map);
    }
    
    /**
     * Verifies the mapping result is correct.
     * 
     * @param name
     *            the path to the resource file.
     * @param obj
     *            the java object to map
     * @throws Exception
     *             if any error occurs.
     */
    private void verify(String name, Object obj) throws Exception
    {
        Element test_case_elm = (Element) XmlUtil.parseDomNode(IoUtil.getResourceAsString(name + ".xml"));
        Element schema_element = (Element) test_case_elm.getChildNodes().item(0);
        Element data_element = (Element) test_case_elm.getChildNodes().item(1);
        SchemaTree schema = TypeXmlParser.parseSchemaTree(schema_element);
        DataTree expected_data = ValueXmlParser.parseDataTree(data_element, schema);
        
        DataTree data_tree = (DataTree) JavaToSqlMapUtil.map(schema, obj);
        AbstractTestCase.assertEqualSerialization(expected_data.getRootValue(), data_tree.getRootValue());
    }
}
