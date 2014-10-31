/**
 * 
 */
package edu.ucsd.forward.data.type;

import org.testng.annotations.Test;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.test.AbstractTestCase;

/**
 * @author Kevin Zhao
 * 
 */
@Test
public class TestTupleType extends AbstractTestCase
{
    private static final Logger log            = Logger.getLogger(TestTupleType.class);
    
    public static final int     NUM_ATTRIBUTES = 15;
    
    public static final int     NUM_RUNS       = 200000;
    
    /**
     * Checking that the iterator interface of TupleType is indeed faster.
     */
    @Test(groups = AbstractTestCase.PERFORMANCE)
    public void testIteratorIsFaster()
    {
        // cool run first
        isIteratorFaster(NUM_ATTRIBUTES, NUM_ATTRIBUTES);
        
        if (!isIteratorFaster(NUM_ATTRIBUTES, NUM_ATTRIBUTES))
        {
            // Give it a second chance instead of declaring a test case failure.
            log.warn("TupleType.iterator() is not faster! Try again..");
            assertTrue(isIteratorFaster(NUM_ATTRIBUTES, NUM_ATTRIBUTES));
        }
    }
    
    private boolean isIteratorFaster(int num_attributes, int num_runs)
    {
        TupleType tuple = new TupleType();
        for (int i = 0; i < num_attributes; i++)
        {
            tuple.setAttribute("name" + i, new StringType());
        }
        
        long name_time = System.nanoTime();
        for (int i = 0; i < num_runs; i++)
        {
            for (String name : tuple.getAttributeNames())
            {
                assertTrue(tuple.getAttribute(name) instanceof StringType);
                assertTrue(name.startsWith("name"));
            }
        }
        name_time = System.nanoTime() - name_time;
        
        long iterator_time = System.nanoTime();
        for (int i = 0; i < num_runs; i++)
        {
            for (TupleType.AttributeEntry entry : tuple)
            {
                assertTrue(entry.getType() instanceof StringType);
                assertTrue(entry.getName().startsWith("name"));
            }
        }
        iterator_time = System.nanoTime() - iterator_time;
        
        log.debug("Iterator time {} v.s. Name time {}", iterator_time, name_time);
        
        if (iterator_time < name_time)
        {
            log.warn("Iterator time is faster: Iterator time {} v.s. Name time {}", iterator_time, name_time);
            return true;
        }
        else
        {
            log.warn("Iterator time is slower: Iterator time {} v.s. Name time {}", iterator_time, name_time);
            return false;
        }
    }
}
