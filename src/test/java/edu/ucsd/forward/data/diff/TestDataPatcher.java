/**
 * 
 */
package edu.ucsd.forward.data.diff;

import org.testng.annotations.Test;
import com.google.gwt.xml.client.Element;

import edu.ucsd.app2you.util.IoUtil;
import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.DataTestCaseUtil;
import edu.ucsd.forward.data.ValueUtil;
import edu.ucsd.forward.data.diff.DataPatcher;
import edu.ucsd.forward.data.diff.DeleteDiff;
import edu.ucsd.forward.data.diff.Diff;
import edu.ucsd.forward.data.diff.InsertDiff;
import edu.ucsd.forward.data.diff.ReplaceDiff;
import edu.ucsd.forward.data.type.SchemaTree;
import edu.ucsd.forward.data.value.DataTree;
import edu.ucsd.forward.data.value.Value;
import edu.ucsd.forward.data.xml.TypeXmlParser;
import edu.ucsd.forward.data.xml.ValueXmlParser;
import edu.ucsd.forward.test.AbstractTestCase;
import edu.ucsd.forward.xml.XmlParserException;
import edu.ucsd.forward.xml.XmlUtil;

/**
 * Tests the data patcher.
 * 
 * @author Kian Win Ong
 * 
 */
@Test
public class TestDataPatcher extends AbstractTestCase
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(TestDataPatcher.class);
    
    /**
     * Tests patching with an insert diff.
     * 
     * @throws XmlParserException
     *             exception.
     */
    public void testInsert() throws XmlParserException
    {
        checkDataPatcher("TestDataPatcher-testInsert.xml", InsertDiff.class, "/set/tuple[id=4]");
    }
    
    /**
     * Tests patching with a replace diff.
     * 
     * @throws XmlParserException
     *             exception.
     */
    public void testReplace() throws XmlParserException
    {
        checkDataPatcher("TestDataPatcher-testReplace.xml", ReplaceDiff.class, "/set/tuple[id=2]/string");
    }
    
    /**
     * Tests patching with a delete diff.
     * 
     * @throws XmlParserException
     *             exception.
     */
    public void testDelete() throws XmlParserException
    {
        checkDataPatcher("TestDataPatcher-testDelete.xml", DeleteDiff.class, "/set/tuple[id=1]");
    }
    
    /**
     * Checks the data patcher results.
     * 
     * @param test_file
     *            the test file which contains the old and new data trees.
     * @param diff_class
     *            the diff class to test.
     * @param context_string
     *            the context of the target value of the diff.
     * @throws XmlParserException
     *             exception.
     */
    private void checkDataPatcher(String test_file, Class<? extends Diff> diff_class, String context_string)
            throws XmlParserException
    {
        Element source_elem = (Element) XmlUtil.parseDomNode(IoUtil.getResourceAsString(this.getClass(), test_file));
        SchemaTree schema_tree = TypeXmlParser.parseSchemaTree(XmlUtil.getOnlyChildElement(source_elem, "schema_tree"));
        
        // Build the old and new data trees
        DataTree old_data = ValueXmlParser.parseDataTree(XmlUtil.getChildElements(source_elem, "data_tree").get(0), schema_tree);
        DataTree new_data = ValueXmlParser.parseDataTree(XmlUtil.getChildElements(source_elem, "data_tree").get(1), schema_tree);
        
        Diff diff = null;
        if (diff_class.equals(InsertDiff.class))
        {
            // Find the target value in the new data tree
            Value new_value = DataTestCaseUtil.find(new_data, context_string);
            diff = new InsertDiff(new_value);
        }
        else if (diff_class.equals(ReplaceDiff.class))
        {
            // Find the target value in the new data tree
            Value new_value = DataTestCaseUtil.find(new_data, context_string);
            diff = new ReplaceDiff(new_value);
        }
        else
        {
            // Find the target value in the old data tree
            assert (diff_class.equals(DeleteDiff.class));
            Value old_value = DataTestCaseUtil.find(old_data, context_string);
            diff = new DeleteDiff(old_value);
        }
        
        // Patch the old data tree
        DataPatcher p = new DataPatcher();
        p.patch(old_data, diff);
        
        // The old data tree should now be equal to the new data tree
        assertTrue(ValueUtil.deepEquals(old_data, new_data));
    }
}
