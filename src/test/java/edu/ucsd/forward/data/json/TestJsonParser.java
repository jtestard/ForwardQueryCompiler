/**
 * 
 */
package edu.ucsd.forward.data.json;

import org.testng.annotations.Test;

import edu.ucsd.app2you.util.IoUtil;
import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.TypeUtil;
import edu.ucsd.forward.data.ValueTypeMapUtil;
import edu.ucsd.forward.data.ValueUtil;
import edu.ucsd.forward.data.json.ValueJsonParser;
import edu.ucsd.forward.data.source.UnifiedApplicationState;
import edu.ucsd.forward.data.type.SchemaTree;
import edu.ucsd.forward.data.value.DataTree;
import edu.ucsd.forward.query.AbstractQueryTestCase;

/**
 * Tests parsing JSON into FORWARD data model.
 * 
 * @author Hongping Lim
 * 
 */
@Test
public class TestJsonParser extends AbstractQueryTestCase
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(TestJsonParser.class);
    
    /**
     * Tests parsing a value.
     * 
     * @throws Exception
     *             if an exception occurs.
     */
    public void testValueJsonSerializer() throws Exception
    {
        parseTestCase(this.getClass(), "TestJsonSerializer-testValue.xml");
        
        UnifiedApplicationState uas = getUnifiedApplicationState();
        
        SchemaTree schema_tree = uas.getDataSource("src_1").getSchemaObject("complex").getSchemaTree();
        DataTree expected = uas.getDataSource("src_1").getDataObject("complex").getDataTree();
        
        String json_string = IoUtil.getResourceAsString("TestJsonSerializer-testValueComplex.json");
        DataTree actual = ValueJsonParser.parseDataTree(json_string, schema_tree, false);
        
        // Setting the type is necessary for calculating primary key
        ValueTypeMapUtil.map(actual, TypeUtil.clone(schema_tree));
        assertTrue(ValueUtil.deepEquals(expected, actual));
    }
    
    /**
     * Tests parsing a value that contains predicates for collection tuples.
     * 
     * @throws Exception
     *             if an exception occurs.
     */
    public void testValueJsonSerializerWithPredicate() throws Exception
    {
        parseTestCase(this.getClass(), "TestJsonSerializer-testPredicate.xml");
        
        UnifiedApplicationState uas = getUnifiedApplicationState();
        
        SchemaTree schema_tree = uas.getDataSource("src_1").getSchemaObject("complex").getSchemaTree();
        DataTree expected = uas.getDataSource("src_1").getDataObject("complex").getDataTree();
        
        String json_string = IoUtil.getResourceAsString("TestJsonSerializer-testPredicate.json");
        DataTree actual = ValueJsonParser.parseDataTree(json_string, schema_tree);
        
        // Setting the type is necessary for calculating primary key
        ValueTypeMapUtil.map(actual, TypeUtil.clone(schema_tree));
        assertTrue(ValueUtil.deepEquals(expected, actual));
    }
}
