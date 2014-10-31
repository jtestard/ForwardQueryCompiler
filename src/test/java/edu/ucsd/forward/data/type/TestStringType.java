/**
 * 
 */
package edu.ucsd.forward.data.type;

import org.testng.annotations.Test;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.TypeUtil;
import edu.ucsd.forward.test.AbstractTestCase;

/**
 * Unit tests for string types.
 * 
 * @author Kian Win
 * 
 */
@Test
public class TestStringType extends AbstractTestCase
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(TestStringType.class);
    
    /**
     * Tests equality.
     * 
     */
    public void testEquals()
    {
        // Two string types are always equal
        assertTrue(TypeUtil.deepEqualsByIsomorphism(new StringType(), new StringType()));
        
        // Sanity check =)
        assertFalse(TypeUtil.deepEqualsByIsomorphism(new StringType(), new IntegerType()));        
    }
}
