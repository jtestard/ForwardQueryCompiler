/**
 * 
 */
package edu.ucsd.forward.query.performance.emailapp;

import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

import javax.swing.JButton;
import javax.swing.JFrame;

import org.testng.annotations.Test;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.query.performance.AbstractPerformanceTestCase;
import edu.ucsd.forward.test.AbstractTestCase;

/**
 * The email application performance test.
 * 
 * @author Yupeng
 * 
 */
@Test(groups = AbstractTestCase.PERFORMANCE)
public class TestPerformance extends AbstractPerformanceTestCase
{
    private static final Logger log = Logger.getLogger(TestPerformance.class);
    
    public void testTopRecentMessages() throws Exception
    {
        run("TestPerformance-testTopRecentMessages");
        run("TestPerformance-testTopRecentMessagesJDBC");
    }

    public void testTotalMessageCount() throws Exception
    {
        run("TestPerformance-testTotalMessageCount");
        run("TestPerformance-testTotalMessageCountJDBC");
    }
    
    public void testMessagesFromGivenSender() throws Exception
    {
//        run("TestPerformance-testMessagesFromGivenSender");
        run("TestPerformance-testMessagesFromGivenSenderJDBC");
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
            parseTestCase(TestPerformance.class, "TestPerformance-testMessagesFromGivenSender.xml");
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
