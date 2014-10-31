/**
 * 
 */
package edu.ucsd.forward.data.index;

import java.util.Arrays;

import org.testng.annotations.Test;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.SchemaPath;
import edu.ucsd.forward.data.source.DataSource;
import edu.ucsd.forward.data.value.CollectionValue;
import edu.ucsd.forward.data.value.DataTree;
import edu.ucsd.forward.data.value.IntegerValue;
import edu.ucsd.forward.data.value.StringValue;
import edu.ucsd.forward.data.value.TupleValue;
import edu.ucsd.forward.exception.CheckedException;
import edu.ucsd.forward.query.AbstractQueryTestCase;

/**
 * Test case for testing indices on data source.
 * 
 * @author Yupeng
 * 
 */
@Test
public class TestIndex extends AbstractQueryTestCase
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(TestIndex.class);
    
    /**
     * Tests the index on the in memory data source.
     * 
     * @throws CheckedException
     *             if anything wrong happens.
     */
    public void testInMemorySource() throws CheckedException
    {
        String relative_file_name = "TestIndex-testInMemorySource";
        // Parse the test case from the XML file
        parseTestCase(this.getClass(), relative_file_name + ".xml");
        
        DataSource data_src = getUnifiedApplicationState().getDataSource("mem");
        DataTree data_tree = data_src.getDataObject("app");
        SchemaPath path_to_coll = new SchemaPath(data_tree.getSchemaTree().getRootType());
        CollectionValue collection = (CollectionValue) data_tree.getRootValue();
        
        // Test retrieve
        String name = "name";
        String id = "appid";
        Index name_idx = data_tree.getIndex(path_to_coll, name);
        assertEquals(2, name_idx.size());
        Index key_idx = data_tree.getIndex(path_to_coll, id);
        assertEquals(3, key_idx.size());
        // Test key range
        assertEquals(1, key_idx.get(Arrays.asList(KeyRange.only(new IntegerValue(3)))).size());
        assertEquals(0, key_idx.get(Arrays.asList(KeyRange.upperBound(new IntegerValue(1), true))).size());
        assertEquals(1, key_idx.get(Arrays.asList(KeyRange.upperBound(new IntegerValue(1), false))).size());
        assertEquals(0, key_idx.get(Arrays.asList(KeyRange.lowerBound(new IntegerValue(3), true))).size());
        assertEquals(1, key_idx.get(Arrays.asList(KeyRange.lowerBound(new IntegerValue(3), false))).size());
        assertEquals(2, key_idx.get(Arrays.asList(KeyRange.bound(new IntegerValue(1), new IntegerValue(3), false, true))).size());
        // Test DML
        TupleValue new_tuple = new TupleValue();
        new_tuple.setAttribute("application_id", new IntegerValue(4));
        new_tuple.setAttribute("first_name", new StringValue("Tom"));
        new_tuple.setAttribute("last_name", new StringValue("Ford"));
        collection.add(new_tuple);
        IndexUtil.addToIndex(collection, new_tuple);
        assertEquals(3,
                     data_src.getFromIndex("app", path_to_coll, name, Arrays.asList(KeyRange.only(new StringValue("Tom")))).size());
        TupleValue removed_tuple = collection.remove(0);
        IndexUtil.removeFromIndex(collection, removed_tuple);
        assertEquals(2,
                     data_src.getFromIndex("app", path_to_coll, name, Arrays.asList(KeyRange.only(new StringValue("Tom")))).size());
        // Test remove index
        data_src.deleteIndex("app", name, path_to_coll);
        assertEquals(1, data_tree.getIndices(path_to_coll).size());
    }
    
    public void testJdbcSource() throws Exception
    {
        String relative_file_name = "TestIndex-testJdbcSource";
        // Parse the test case from the XML file
        parseTestCase(this.getClass(), relative_file_name + ".xml");
        
        DataSource data_src = getUnifiedApplicationState().getDataSource("jdbc");
        // Delete index
        data_src.deleteIndex("app", "name", new SchemaPath(""));
        
    }
}
