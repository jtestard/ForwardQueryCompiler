/**
 * 
 */
package edu.ucsd.forward.data;

import java.text.ParseException;

import org.testng.annotations.Test;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.value.DataTree;
import edu.ucsd.forward.data.value.Value;
import edu.ucsd.forward.data.xml.TestValueTypeXmlParser;
import edu.ucsd.forward.test.AbstractTestCase;

/**
 * The test cases for the data path and schema path.
 * 
 * @author Yupeng
 * 
 */
@Test
public class TestPath extends AbstractTestCase
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(TestPath.class);
    
    /**
     * Tests DataPath.find().
     * 
     * @throws Exception
     *             exception
     */
    public void testIsValidPath() throws Exception
    {
        DataTree input = TestValueTypeXmlParser.readDataAndSchema(TestPath.class, "TestPath-testIsValidPath.xml");
        
        assertTrue(isValidPath(input, "apply"));
        assertTrue(isValidPath(input, "apply/applying_for/phd/advisor_choice"));
        assertFalse(isValidPath(input, "apply/applying_for/masters"));
        assertFalse(isValidPath(input, "no/such/path"));
    }
    
    /**
     * Finds the value in the data tree with the given path.
     * 
     * @param data
     *            data tree
     * @param path
     *            path
     * @return true if found, else false
     * @throws ParseException
     *             exception
     */
    public boolean isValidPath(DataTree data, String path) throws ParseException
    {
        DataPath p = new DataPath(path);
        Value v = p.find(data);
        return v != null;
    }
}
