/**
 * 
 */
package edu.ucsd.forward.data.diff;

import java.util.List;

import org.testng.annotations.Test;
import com.google.gwt.xml.client.Element;

import edu.ucsd.app2you.util.IoUtil;
import edu.ucsd.forward.data.diff.DataComparer;
import edu.ucsd.forward.data.diff.Diff;
import edu.ucsd.forward.data.type.SchemaTree;
import edu.ucsd.forward.data.value.DataTree;
import edu.ucsd.forward.data.xml.DataDiffXmlSerializer;
import edu.ucsd.forward.data.xml.TypeXmlParser;
import edu.ucsd.forward.data.xml.ValueXmlParser;
import edu.ucsd.forward.test.AbstractTestCase;
import edu.ucsd.forward.xml.XmlParserException;
import edu.ucsd.forward.xml.XmlUtil;

/**
 * Unit tests for a data comparer.
 * 
 * @author Hongping Lim
 * 
 */
@Test
public class TestDataComparer extends AbstractTestCase
{
    
    private static final String TEST_CASE_PREFIX        = "TestDataComparer-";
    
    private static final String TEST_CASE_INPUT_SUFFIX  = "-input.xml";
    
    private static final String TEST_CASE_OUTPUT_SUFFIX = "-output.xml";
    
    /**
     * Tests comparing two tuples.
     * 
     * @throws XmlParserException
     *             xml exception
     */
    public void testTuples() throws XmlParserException
    {
        verifyTestCase("testTuples");
    }
    
    /**
     * Tests comparing two collections and replacing scalar values.
     * 
     * @throws XmlParserException
     *             xml exception
     */
    public void testCollections() throws XmlParserException
    {
        verifyTestCase("testCollections");
    }
    
    /**
     * Tests deleting a tuple from a collection.
     * 
     * @throws XmlParserException
     *             xml exception
     */
    public void testDelete() throws XmlParserException
    {
        verifyTestCase("testDelete");
    }
    
    /**
     * Tests inserting a tuple into the start of a collection. The whole collection should get replaced because we have to delete
     * the existing tuples and insert the first tuple before reinserting the existing tuples.
     * 
     * @throws XmlParserException
     *             xml exception
     */
    public void testInsertFirst() throws XmlParserException
    {
        verifyTestCase("testInsertFirst");
    }
    
    /**
     * Tests inserting a tuple into the middle of a collection. Only the existing tuples after the position should get deleted, so
     * that we can insert the new tuple before reinserting the existing tuples.
     * 
     * @throws XmlParserException
     *             xml exception
     */
    public void testInsertBetween() throws XmlParserException
    {
        verifyTestCase("testInsertBetween");
    }
    
    /**
     * Tests inserting a tuple into the end of a collection. Nothing needs to be deleted as the new tuple can just be appended to
     * the end.
     * 
     * @throws XmlParserException
     *             xml exception
     */
    public void testInsertLast() throws XmlParserException
    {
        verifyTestCase("testInsertLast");
    }
    
    /**
     * Tests inserting new tuples into an empty collection. The whole collection can simply be replaced.
     * 
     * @throws XmlParserException
     *             xml exception
     */
    public void testInsert() throws XmlParserException
    {
        verifyTestCase("testInsert");
    }
    
    /**
     * Tests deleting all tuples into an empty collection. The whole collection can simply be replaced.
     * 
     * @throws XmlParserException
     *             xml exception
     */
    public void testDeleteAll() throws XmlParserException
    {
        verifyTestCase("testDeleteAll");
    }
    
    /**
     * Tests reordering of two tuples. The affected tuple is to be deleted and then reinserted at the end.
     * 
     * @throws XmlParserException
     *             xml exception
     */
    public void testReorder() throws XmlParserException
    {
        verifyTestCase("testReorder");
    }
    
    /**
     * Tests reordering of three out of four tuples. The two affected tuples are to be deleted and then reinserted at the end.
     * 
     * @throws XmlParserException
     *             xml exception
     */
    public void testReorderMultiple() throws XmlParserException
    {
        verifyTestCase("testReorderMultiple");
    }
    
    /**
     * Tests reordering of all three tuples. The first two tuples are to be deleted and then inserted at the end in the new order.
     * 
     * @throws XmlParserException
     *             xml exception
     */
    public void testReorderAll() throws XmlParserException
    {
        verifyTestCase("testReorderAll");
    }
    
    /**
     * Performs running and verification of the test case.
     * 
     * @param name
     *            test case name
     * @throws XmlParserException
     *             xml exception
     */
    private void verifyTestCase(String name) throws XmlParserException
    {
        
        Element test_case_elm = (Element) XmlUtil.parseDomNode(IoUtil.getResourceAsString(TEST_CASE_PREFIX + name
                + TEST_CASE_INPUT_SUFFIX));
        Element schema_element = (Element) test_case_elm.getChildNodes().item(0);
        Element data_element_1 = (Element) test_case_elm.getChildNodes().item(1);
        Element data_element_2 = (Element) test_case_elm.getChildNodes().item(2);
        
        SchemaTree schema = TypeXmlParser.parseSchemaTree(schema_element);
        DataTree data1 = ValueXmlParser.parseDataTree(data_element_1, schema);
        DataTree data2 = ValueXmlParser.parseDataTree(data_element_2, schema);
        
        DataComparer comparer = new DataComparer(data1, data2);
        List<Diff> diffs = comparer.compare();
        DataDiffXmlSerializer serializer = new DataDiffXmlSerializer();
        String actual = XmlUtil.serializeDomToString(serializer.serialize(diffs), false, true);
        String expected = IoUtil.getResourceAsString(TEST_CASE_PREFIX + name + TEST_CASE_OUTPUT_SUFFIX);
        assertEquals(IoUtil.normalizeLineBreak(expected), IoUtil.normalizeLineBreak(actual));
    }
}
