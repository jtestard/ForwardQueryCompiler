/**
 * 
 */
package edu.ucsd.forward.data.encoding;

import java.io.IOException;

import org.testng.annotations.Test;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.type.CollectionType;
import edu.ucsd.forward.data.type.IntegerType;
import edu.ucsd.forward.data.type.StringType;
import edu.ucsd.forward.data.type.TupleType;
import edu.ucsd.forward.data.type.Type;
import edu.ucsd.forward.test.AbstractTestCase;

/**
 * @author Aditya Avinash
 * TODO: Write unit test case for all methods defined in {@code SchemaBlock}
 */
@Test
public class TestSchemaBlock extends AbstractTestCase
{
    @SuppressWarnings("unused")
    private static final Logger  log                   = Logger.getLogger(TestSchemaBlock.class);
    
    private static final String  TEST_ATTRIBUTE_NAME_1 = "name";
    
    private static final String  TEST_ATTRIBUTE_NAME_2 = "id";
    
    private static final String  TEST_ATTRIBUTE_NAME_3 = "email";
    
    private static final boolean NOT_ORDERED           = false;
    
    /**
     * 
     * 
     * @throws IOException
     */
    public void testSchema() throws IOException
    
    {
        CollectionType collectionType = new CollectionType();
        collectionType.setOrdered(NOT_ORDERED);
        TupleType tupleType = new TupleType();
        tupleType.setAttribute(TEST_ATTRIBUTE_NAME_1, new StringType());
        tupleType.setAttribute(TEST_ATTRIBUTE_NAME_2, new IntegerType());
        tupleType.setAttribute(TEST_ATTRIBUTE_NAME_3, new StringType());
        collectionType.setChildrenType(tupleType);
        
        SchemaBlock schemaBlock = new SchemaBlock();
        int type_id = schemaBlock.getUniqueTypeID();
        schemaBlock.putType(type_id, collectionType);
        Type originalType = collectionType;
        Type decodedType = schemaBlock.getType(type_id);
        assertTrue("Schema type decoded is incorrect", originalType.getClass().equals(decodedType.getClass()));
        Type originalChildrenType = ((CollectionType)originalType).getChildrenType();
        Type decodedChildrenType = ((CollectionType)decodedType).getChildrenType();
        assertTrue("Schema type decoded is incorrect", originalChildrenType.getClass().equals(decodedChildrenType.getClass()));
        assertTrue("Some attributes have not been decoded properly from Tuple Type", ((TupleType)originalChildrenType).getSize() == ((TupleType)originalChildrenType).getSize() );
        assertTrue("Schema type decoded is incorrect", originalType.toString().equals(decodedType.toString()));
        
    }
    
}
