/**
 * 
 */
package edu.ucsd.forward.data;

import java.util.ArrayList;
import java.util.List;

import org.testng.annotations.Test;

import com.google.gwt.xml.client.Element;

import edu.ucsd.app2you.util.IoUtil;
import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.type.SchemaTree;
import edu.ucsd.forward.data.value.DataTree;
import edu.ucsd.forward.data.value.IntegerValue;
import edu.ucsd.forward.data.value.ScalarValue;
import edu.ucsd.forward.data.xml.TypeXmlParser;
import edu.ucsd.forward.data.xml.ValueXmlParser;
import edu.ucsd.forward.test.AbstractTestCase;
import edu.ucsd.forward.xml.XmlParserException;
import edu.ucsd.forward.xml.XmlUtil;

/**
 * Unit tests for <code>ContextUtil</code>.
 * 
 * @author Kian Win
 * 
 */
@Test
public class TestContextUtil extends AbstractTestCase
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(TestContextUtil.class);
    
    /**
     * Tests getting the context of all possible kinds of data nodes as a list of values.
     * 
     * @throws XmlParserException
     *             exception.
     */
    public void testGetContextAsValue() throws XmlParserException
    {
        String source = "TestContextUtil-testGetContextAsValue.xml";
        Element source_elem = (Element) XmlUtil.parseDomNode(IoUtil.getResourceAsString(this.getClass(), source));
        SchemaTree schema_tree = TypeXmlParser.parseSchemaTree(XmlUtil.getOnlyChildElement(source_elem, "schema_tree"));
        DataTree data_tree = ValueXmlParser.parseDataTree(XmlUtil.getOnlyChildElement(source_elem, "data_tree"), schema_tree);
        
        // The dataset, dataset tuple and top-level values have only an empty list
        assertTrue(compareListOfScalarValues(newIds(), ContextUtil.getContextAsValue(data_tree).getScalarValues()));
        assertTrue(compareListOfScalarValues(newIds(), ContextUtil.getContextAsValue(data_tree.getRootValue()).getScalarValues()));
        assertTrue(compareListOfScalarValues(newIds(), getIds(data_tree, "user_id")));
        assertTrue(compareListOfScalarValues(newIds(), getIds(data_tree, "application")));
        
        // A tuple of the top-level relation
        assertTrue(compareListOfScalarValues(newIds(123), getIds(data_tree, "application/0")));
        
        // The global primary key attribute itself
        assertTrue(compareListOfScalarValues(newIds(123), getIds(data_tree, "application/0/application_id")));
        
        // An ordinary scalar value
        assertTrue(compareListOfScalarValues(newIds(123), getIds(data_tree, "application/0/first_name")));
        
        // A switch value and its descendants
        assertTrue(compareListOfScalarValues(newIds(123), getIds(data_tree, "application/0/applying_for")));
        assertTrue(compareListOfScalarValues(newIds(123), getIds(data_tree, "application/0/applying_for/phd")));
        assertTrue(compareListOfScalarValues(newIds(123), getIds(data_tree, "application/0/applying_for/phd/specialization")));
        
        // A nested relation
        assertTrue(compareListOfScalarValues(newIds(123), getIds(data_tree, "application/0/degrees")));
        
        // Tuples of the nested relation, and values in them
        assertTrue(compareListOfScalarValues(newIds(123, 1), getIds(data_tree, "application/0/degrees/0")));
        assertTrue(compareListOfScalarValues(newIds(123, 1), getIds(data_tree, "application/0/degrees/0/degree_id")));
        assertTrue(compareListOfScalarValues(newIds(123, 1), getIds(data_tree, "application/0/degrees/0/school")));
        assertTrue(compareListOfScalarValues(newIds(123, 3), getIds(data_tree, "application/0/degrees/1")));
        assertTrue(compareListOfScalarValues(newIds(123, 3), getIds(data_tree, "application/0/degrees/1/degree_id")));
        assertTrue(compareListOfScalarValues(newIds(123, 3), getIds(data_tree, "application/0/degrees/1/school")));
    }
    
    /**
     * Returns a list of id values for the context of a data node.
     * 
     * @param dataset
     *            - the dataset of the data node.
     * @param path
     *            - the hierarchical path to navigate to the data node.
     * @return the list of id values.
     */
    protected static List<ScalarValue> getIds(DataTree dataset, String path)
    {
        return ContextUtil.getContextAsValue(ValueUtil.get(dataset, path)).getScalarValues();
    }
    
    /**
     * Constructs a list of integer id values from given integers.
     * 
     * @param integers
     *            the integers.
     * @return the list of integer id values.
     */
    protected static List<IntegerValue> newIds(Integer... integers)
    {
        List<IntegerValue> list = new ArrayList<IntegerValue>();
        for (Integer integer : integers)
        {
            list.add(new IntegerValue(integer));
        }
        return list;
    }
    
    /**
     * Compares two lists of scalar values by value.
     * 
     * @param list1
     *            the first list
     * @param list2
     *            the second list
     * @return <code>true</code> if the two list contain the same scalar values, <code>false</code> otherwise.
     */
    protected static boolean compareListOfScalarValues(List<? extends ScalarValue> list1, List<? extends ScalarValue> list2)
    {
        if (list1.size() != list2.size()) return false;
        for (int i = 0; i < list1.size(); i++)
        {
            if (!list1.get(i).toString().endsWith(list2.get(i).toString())) return false;
        }
        return true;
    }
}
