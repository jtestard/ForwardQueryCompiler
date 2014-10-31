/**
 * 
 */
package edu.ucsd.forward.xml;

import org.testng.annotations.Test;

import com.google.gwt.xml.client.Document;
import com.google.gwt.xml.client.Element;
import com.google.gwt.xml.client.Text;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.test.AbstractTestCase;
import edu.ucsd.forward.xml.DomApiProxy;

/**
 * A test class for GWT DOM wrapper.
 * 
 * @author Yupeng
 * 
 */
@Test
public class TestGwtDom extends AbstractTestCase
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(TestGwtDom.class);
    
    /**
     * Tests basic DOM operation.
     */
    public void testBasic()
    {
        Document doc = DomApiProxy.makeDocument();
        Element element = doc.createElement("test");
        Text text = doc.createTextNode("text");
        element.appendChild(text);
        String expected_str = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<test>text</test>";
        assertEquals(DomApiProxy.serializeDomToString(element, false, false), expected_str);
    }
}
