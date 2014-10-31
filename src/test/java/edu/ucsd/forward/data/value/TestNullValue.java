/**
 * 
 */
package edu.ucsd.forward.data.value;

import org.testng.annotations.Test;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.ValueUtil;
import edu.ucsd.forward.test.AbstractTestCase;

/**
 * Unit tests for null values.
 * 
 * @author Kian Win
 * 
 */
@Test
public class TestNullValue extends AbstractTestCase
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(TestNullValue.class);
    
    /**
     * Tests equality.
     * 
     */
    public void testEquals()
    {
        NullValue x1 = new NullValue();
        NullValue x2 = new NullValue();
        
        // A null value has no state
        assertTrue(ValueUtil.deepEquals(x1, x2));
        
        // Even though NULL = NULL evaluates to FALSE in SQL, Java requires that equals() be reflexive.
        assertTrue(ValueUtil.deepEquals(x1, x1));
    }
}
