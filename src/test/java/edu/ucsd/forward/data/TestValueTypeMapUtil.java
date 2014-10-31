/**
 * 
 */
package edu.ucsd.forward.data;

import java.util.ArrayList;

import org.testng.annotations.Test;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.type.StringType;
import edu.ucsd.forward.data.type.TupleType;
import edu.ucsd.forward.data.value.StringValue;
import edu.ucsd.forward.data.value.TupleValue;
import edu.ucsd.forward.test.AbstractTestCase;

/**
 * The test cases for ValueTypeMapUtil.
 */
@Test
public class TestValueTypeMapUtil extends AbstractTestCase
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(TestValueTypeMapUtil.class);
    
    /**
     * Tests that mapping a tuple with an unset attribute will still result in the value have attributes in the same order as the
     * type.
     */
    public void testTupleAttributesOrder()
    {
        TupleType type = new TupleType();
        type.setAttribute("attr1", new StringType());
        type.setAttribute("attr2", new StringType());
        
        TupleValue value = new TupleValue();
        value.setAttribute("attr2", new StringValue("attr_value2"));
        ValueTypeMapUtil.map(value, type);
        ArrayList<String> type_attrs = new ArrayList<String>(type.getAttributeNames());
        ArrayList<String> value_attrs = new ArrayList<String>(value.getAttributeNames());
        assertEquals(type_attrs, value_attrs);
    }
}
