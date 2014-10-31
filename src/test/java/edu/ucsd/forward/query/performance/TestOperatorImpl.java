/**
 * 
 */
package edu.ucsd.forward.query.performance;

import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

import javax.swing.JButton;
import javax.swing.JFrame;

import org.testng.annotations.Test;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.test.AbstractTestCase;

/**
 * The micro-benchmark that tests the performance of each operator implementation.
 * 
 * @author Yupeng
 * @author Michalis Petropoulos
 * 
 */
@Test(groups = AbstractTestCase.PERFORMANCE)
public class TestOperatorImpl extends AbstractPerformanceTestCase
{
    private static final Logger log = Logger.getLogger(TestOperatorImpl.class);
    
    /**
     * Tests the performance of reading bindings from a data object.
     * 
     * @throws Exception
     *             if something goes wrong.
     */
    public void testDataObject() throws Exception
    {
        run("TestOperatorImpl-testDataObjectSmall");
        // run("TestOperatorImpl-testDataObjectLarge");
    }
    
    /**
     * Tests the performance of inner join operator.
     * 
     * @throws Exception
     *             if something goes wrong.
     */
    public void testInnerJoin() throws Exception
    {
        run("TestOperatorImpl-testInnerJoinLeftSmallRightLarge");
    }
    
    /**
     * Tests the performance of union operator.
     * 
     * @throws Exception
     *             if something goes wrong.
     */
    public void testUnion() throws Exception
    {
        run("TestOperatorImpl-testUnion");
    }
    
    /**
     * Tests the performance of union operator.
     * 
     * @throws Exception
     *             if something goes wrong.
     */
    public void testUnionAll() throws Exception
    {
        run("TestOperatorImpl-testUnionAll");
    }
    
    /**
     * Tests the performance of intersect operator.
     * 
     * @throws Exception
     *             if something goes wrong.
     */
    public void testIntersect() throws Exception
    {
        run("TestOperatorImpl-testIntersect");
    }
    
    /**
     * Tests the performance of left outer join operator.
     * 
     * @throws Exception
     *             if something goes wrong.
     */
    public void testLeftOuterJoin() throws Exception
    {
        // run("TestOperatorImpl-testLeftOuterJoinLeftSmall");
        // run("TestOperatorImpl-testLeftOuterJoinLeftLarge");
        run("TestOperatorImpl-testLeftOuterJoinLeftSmallRightLarge");
    }
    
    /**
     * Tests the performance of right outer join operator.
     * 
     * @throws Exception
     *             if something goes wrong.
     */
    public void testRightOuterJoin() throws Exception
    {
        run("TestOperatorImpl-testRightOuterJoin");
    }
    
    /**
     * Tests the performance of sort operator.
     * 
     * @throws Exception
     *             if something goes wrong.
     */
    public void testSort() throws Exception
    {
        run("TestOperatorImpl-testSort");
    }
    
    /**
     * Tests the performance of duplicate elimination.
     * 
     * @throws Exception
     *             if something goes wrong.
     */
    public void testEliminateDuplicates() throws Exception
    {
        run("TestOperatorImpl-testEliminateDuplicates");
    }
    
    /**
     * Tests the performance of the fetch operator.
     * 
     * @throws Exception
     *             if something goes wrong.
     */
    public void testFetch() throws Exception
    {
        run("TestOperatorImpl-testFetch");
    }
    
    /**
     * Tests the performance of the group by operator and aggregate function.
     * 
     * @throws Exception
     *             if something goes wrong.
     */
    public void testGroupBy() throws Exception
    {
        run("TestOperatorImpl-testGroupBy");
    }
    
    /**
     * Tests the performance of the apply plan operator.
     * 
     * @throws Exception
     *             if something goes wrong.
     */
    public void testApplyPlan() throws Exception
    {
        run("TestOperatorImpl-testApplyPlan");
        // The optimized plan
        runPhysicalPlan("TestOperatorImpl-testApplyPlanOptimized");
    }
    
    /**
     * Tests the performance of the apply plan operator.
     * 
     * @throws Exception
     *             if something goes wrong.
     */
    public void testSendPlan() throws Exception
    {
        run("TestOperatorImpl-testSendPlan");
    }
    
    /**
     * Tests the performance of the exists clause.
     * 
     * @throws Exception
     *             if something goes wrong.
     */
    public void testExists() throws Exception
    {
        run("TestOperatorImpl-testExists");
    }
    
    /**
     * Tests the performance of the not-exists clause.
     * 
     * @throws Exception
     *             if something goes wrong.
     */
    public void testNotExists() throws Exception
    {
        run("TestOperatorImpl-testNotExists");
    }
    
    /**
     * Tests the performance of the plan of semi-join.
     * 
     * @throws Exception
     *             if something goes wrong.
     */
    public void testSemiJoinPlan() throws Exception
    {
        runPhysicalPlan("TestOperatorImpl-testSemiJoinPlan");
    }
    
    /**
     * Tests the performance of the plan of anti-semi-join.
     * 
     * @throws Exception
     *             if something goes wrong.
     */
    public void testAntiSemiJoinPlan() throws Exception
    {
        runPhysicalPlan("TestOperatorImpl-testAntiSemiJoinPlan");
    }
    
    /**
     * Continuously executes one performance test within a GUI. This method is used for the profiling purpose.
     * 
     * @param args
     *            the arguments
     */
    public static void main(String[] args)
    {
        // Parse the test case from the XML file
        try
        {
            // parseTestCase(TestOperatorImpl.class, "TestOperatorImpl-testGroupBy.xml");
            // parseTestCase(TestOperatorImpl.class, "TestOperatorImpl-testEliminateDuplicates.xml");
            // parseTestCase(TestOperatorImpl.class, "TestOperatorImpl-testDataObjectSmall.xml");
            // parseTestCase(TestOperatorImpl.class, "TestOperatorImpl-testApplyPlan.xml");
            // parseTestCase(TestOperatorImpl.class, "TestOperatorImpl-testFetch.xml");
            // parseTestCase(TestOperatorImpl.class, "TestOperatorImpl-testSort.xml");
            // parseTestCase(TestOperatorImpl.class, "TestOperatorImpl-testUnion.xml");
            // parseTestCase(TestOperatorImpl.class, "TestOperatorImpl-testInnerJoinLeftSmallRightLarge.xml");
            parseTestCase(TestOperatorImpl.class, "TestOperatorImpl-testLeftOuterJoinLeftSmallRightLarge.xml");
        }
        catch (Exception e)
        {
            log.error(e.getMessage());
            throw new AssertionError();
        }
        
        JButton jb = new JButton("Press Me");
        
        jb.addActionListener(new ActionListener()
        {
            public void actionPerformed(ActionEvent ev)
            {
                try
                {
                    measure();
                }
                catch (Exception e)
                {
                    log.error(e.getMessage());
                    throw new AssertionError();
                }
            }
        });
        
        JFrame f = new JFrame();
        f.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        f.getContentPane().add(jb);
        f.pack();
        f.setVisible(true);
    }
}
